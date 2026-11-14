"""补采提交的事务隔离、取消清理与并发查询回归测试。

覆盖需求：
1. 并发合法提交各自整批成功，不出现“事务嵌套”错误，查询读不到半批；
2. 尚未提交时取消：本批任务与明细全部撤销，已提交的完整任务保留；
3. 等待 SQLite 写锁时取消：解锁后不遗留事务、无迟到写入，无需重启即可再提交；
4. 清理不干扰其他请求；
5. 写入等待期间健康检查、进度、已有站日结果仍可响应，且只取自完整已提交状态；
6. 空任务不会被报为完成。
"""
from __future__ import annotations

import asyncio

import pytest

from app import db
from app.hooks import hooks


# ---------------------------------------------------------------- 1. 并发提交
async def test_concurrent_submits_each_commit_whole_batch(client, make_payload):
    n_jobs, n_items = 6, 5
    responses = await asyncio.gather(
        *(
            client.post("/jobs", json=make_payload(n=n_items, prefix=f"S{j}"))
            for j in range(n_jobs)
        )
    )
    assert all(r.status_code == 201 for r in responses), [r.text for r in responses]
    job_ids = [r.json()["job_id"] for r in responses]
    # job_id 各不相同
    assert len(set(job_ids)) == n_jobs

    async with db.read_connection() as conn:
        jobs = await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs")
        items = await db.fetchone(conn, "SELECT COUNT(*) c FROM items")
        assert jobs["c"] == n_jobs
        assert items["c"] == n_jobs * n_items
        # 每个任务都是整批：不存在“有任务无明细”的半批
        partial = await db.fetchall(
            conn,
            "SELECT j.id FROM jobs j LEFT JOIN items i ON i.job_id=j.id "
            "GROUP BY j.id HAVING COUNT(i.id)=0",
        )
        assert partial == []
        # source_states 每个源一行（去重）
        srcs = await db.fetchone(conn, "SELECT COUNT(*) c FROM source_states")
        assert srcs["c"] == 1

    for jid in job_ids:
        prog = (await client.get(f"/jobs/{jid}/progress")).json()
        assert prog["total"] == n_items
        assert prog["pending"] == n_items
        assert prog["done"] is False


async def test_concurrent_different_sources_isolated(client, make_payload):
    responses = await asyncio.gather(
        *(
            client.post(
                "/jobs",
                json=make_payload(n=2, source=f"http://src-{j}:8081", prefix=f"X{j}"),
            )
            for j in range(4)
        )
    )
    assert all(r.status_code == 201 for r in responses)
    async with db.read_connection() as conn:
        srcs = await db.fetchone(conn, "SELECT COUNT(*) c FROM source_states")
        assert srcs["c"] == 4


# ------------------------------------------------- 2. 提交中途取消：整批撤销
async def test_cancel_before_commit_removes_job_and_items(client, make_payload):
    # 放行到“jobs 行已写、items 未写”，此时取消请求
    task = asyncio.create_task(client.post("/jobs", json=make_payload(n=4)))
    await hooks.wait("after_job_insert")

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 0
    # 解除测试闸门，验证取消后无需重启即可继续提交
    hooks.reset()
    r = await client.post("/jobs", json=make_payload(n=2))
    assert r.status_code == 201


async def test_cancel_while_waiting_for_write_lock_prompts_and_rolls_back(
    client, make_payload, lock_holder
):
    lock_holder.acquire()  # 外部进程持有写锁，提交将阻塞在取锁循环
    task = asyncio.create_task(client.post("/jobs", json=make_payload(n=3)))

    # 给取锁循环一点时间进入等待
    await asyncio.sleep(0.3)
    assert not task.done()

    loop = asyncio.get_running_loop()
    t0 = loop.time()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    cancel_elapsed = loop.time() - t0
    # 取消必须在一个短取锁周期内生效，而不是等满 busy_timeout
    assert cancel_elapsed < 1.0, cancel_elapsed

    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 0

    # 解锁后不得有迟到写入，且无需重启即可再提交
    lock_holder.release()
    await asyncio.sleep(0.5)
    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0

    r = await client.post("/jobs", json=make_payload(n=2))
    assert r.status_code == 201
    assert (await client.get(f"/jobs/{r.json()['job_id']}/progress")).json()[
        "total"
    ] == 2


async def test_cancel_does_not_touch_committed_job(client, make_payload):
    # 先完整提交一个任务
    r1 = await client.post("/jobs", json=make_payload(n=3, prefix="A"))
    assert r1.status_code == 201
    committed_id = r1.json()["job_id"]

    # 第二个任务在提交前取消
    task = asyncio.create_task(client.post("/jobs", json=make_payload(n=2, prefix="B")))
    await hooks.wait("after_job_insert")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # 已提交任务完整保留、可查询
    prog = (await client.get(f"/jobs/{committed_id}/progress")).json()
    assert prog["total"] == 3
    items = (await client.get(f"/jobs/{committed_id}/items")).json()
    assert len(items) == 3
    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 1
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 3


# ------------------------------------------------- 3. 等锁期间查询照常响应
async def test_queries_respond_while_write_waits_on_lock(
    client, make_payload, lock_holder
):
    # 先提交一个完整任务并制造一条已成功的站日结果
    r = await client.post("/jobs", json=make_payload(n=2))
    jid = r.json()["job_id"]
    async with db.immediate_transaction() as conn:
        await conn.execute(
            "UPDATE items SET status='succeeded', precipitation=12.5, updated_at=1.0 "
            "WHERE job_id=?",
            (jid,),
        )

    # 外部持锁；另一个提交进入写锁等待
    lock_holder.acquire()
    blocked = asyncio.create_task(
        client.post("/jobs", json=make_payload(n=2, prefix="B"))
    )
    await asyncio.sleep(0.3)
    assert not blocked.done()

    # 等待期间：健康检查、进度、结果、明细、源状态全部即时可响应
    health = await client.get("/health")
    assert health.status_code == 200
    prog = (await client.get(f"/jobs/{jid}/progress")).json()
    assert prog["succeeded"] == 2 and prog["done"] is True
    results = (await client.get(f"/jobs/{jid}/results")).json()
    assert len(results) == 2 and results[0]["precipitation"] == 12.5
    assert len((await client.get(f"/jobs/{jid}/items")).json()) == 2
    assert (await client.get("/sources")).status_code == 200

    # 等待中的半批绝不可见
    not_found = await client.get("/jobs/9999/progress")
    assert not_found.status_code == 404

    # 放行后提交成功，状态恢复一致
    lock_holder.release()
    created = await blocked
    assert created.status_code == 201
    new_id = created.json()["job_id"]
    assert (await client.get(f"/jobs/{new_id}/progress")).json()["total"] == 2


async def test_uncommitted_batch_invisible_to_concurrent_readers(
    client, make_payload
):
    # 让一个提交停在“明细已写、尚未提交”
    blocked = asyncio.create_task(client.post("/jobs", json=make_payload(n=4)))
    await hooks.wait("before_commit")

    # 并发查询：任务尚不存在（未提交快照不可见）
    r404 = await client.get("/jobs/1/progress")
    assert r404.status_code == 404
    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 0

    hooks.set("before_commit")
    created = await blocked
    assert created.status_code == 201
    jid = created.json()["job_id"]
    # 提交后立即可读到完整一批
    prog = (await client.get(f"/jobs/{jid}/progress")).json()
    assert prog["total"] == 4 and prog["pending"] == 4


# ------------------------------------------------- 4. 清理不干扰其他请求
async def test_cancel_cleanup_does_not_distract_other_writes(
    client, make_payload, lock_holder
):
    lock_holder.acquire()
    cancelled = asyncio.create_task(
        client.post("/jobs", json=make_payload(n=2, prefix="C"))
    )
    await asyncio.sleep(0.3)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    # 外部锁持有者自身的写入与提交不受取消清理影响
    lock_holder.conn.execute(
        "INSERT INTO source_states(source_base) VALUES ('http://other:1')"
    )
    lock_holder.release()

    # 另一个请求可正常整批提交
    r = await client.post("/jobs", json=make_payload(n=3, prefix="D"))
    assert r.status_code == 201
    async with db.read_connection() as conn:
        rows = await db.fetchall(conn, "SELECT source_base FROM source_states")
        bases = {row["source_base"] for row in rows}
        assert "http://other:1" in bases


# ------------------------------------------------- 5. 空任务不报完成
async def test_progress_never_reports_empty_job_done(init_schema):
    # 手工制造“只有 jobs 行、没有 items 行”的半批状态（正常原子提交不会产生）
    async with db.immediate_transaction() as conn:
        await conn.execute(
            "INSERT INTO jobs(source_base, created_at) VALUES ('http://x:1', 1.0)"
        )
    from app.api import app as api_app
    import httpx

    transport = httpx.ASGITransport(app=api_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://collector"
    ) as c:
        prog = (await c.get("/jobs/1/progress")).json()
        assert prog["total"] == 0
        assert prog["done"] is False


async def test_validation_errors_do_not_open_transaction(client, make_payload):
    # 重复站日 / 非法 URL / 非法日期都应在开事务前被拒绝
    bad_dup = make_payload(n=2)
    bad_dup["items"][1] = dict(bad_dup["items"][0])
    assert (await client.post("/jobs", json=bad_dup)).status_code == 422
    bad_url = make_payload(n=1)
    bad_url["source_base"] = "ftp://nope"
    assert (await client.post("/jobs", json=bad_url)).status_code == 422
    bad_date = make_payload(n=1)
    bad_date["items"][0]["date"] = "2026-13-40"
    assert (await client.post("/jobs", json=bad_date)).status_code == 422

    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0


async def test_busy_timeout_exhaustion_leaves_clean_state(
    client, make_payload, lock_holder, monkeypatch
):
    import app.db as dbmod
    from app import config

    monkeypatch.setattr(config, "BUSY_TIMEOUT_MS", 600)
    lock_holder.acquire()
    with pytest.raises(Exception):
        async with dbmod.immediate_transaction() as conn:
            await conn.execute("INSERT INTO jobs DEFAULT VALUES")
    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0
    lock_holder.release()
    # 之后仍可立即提交
    r = await client.post("/jobs", json=make_payload(n=1))
    assert r.status_code == 201
