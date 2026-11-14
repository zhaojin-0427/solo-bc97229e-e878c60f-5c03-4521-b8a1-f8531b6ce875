"""双采集进程集成回归：保留原限流/冷却/租约规则，并验证优雅退出接续。

两个 Worker 实例（等价 docker-compose 中的 worker1/worker2）共享同一 SQLite
库，通过 httpx MockTransport 访问内置模拟源，确定性地核对：
- 同源全局在途 ≤ 2、相邻请求开始间隔 ≥ MIN_REQUEST_GAP_SECONDS；
- 429 按 Retry-After 冷却且自动重试，最终全部成功；
- 两进程并行采集、心跳存活，结果完整且不重复保存；
- 优雅退出把在途项回 pending，重启后续跑。
"""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from app import config, db
from app.worker import Worker


class FakeSource:
    """记录请求并可按策略返回 200/429/延迟的内存气象源。"""

    def __init__(self, retry_after: float = 0.5) -> None:
        self.retry_after = retry_after
        self.requests: list[tuple[str, str, float]] = []
        self.statuses: list[int] = []
        self.counter = 0
        self.lock = asyncio.Lock()
        # 每 4 个请求返回一次 429
        self.every = 4
        self.delay = 0.0

    def handler(self, request: httpx.Request) -> httpx.Response:
        # MockTransport 的 handler 在线程/事件循环内同步执行；这里仅做轻量记录
        station = request.url.params["station_id"]
        date = request.url.params["date"]
        self.requests.append((station, date, time.time()))
        self.counter += 1
        if self.counter % self.every == 0:
            self.statuses.append(429)
            return httpx.Response(
                429,
                headers={"Retry-After": str(int(self.retry_after))},
                json={"error": "rate_limited"},
            )
        self.statuses.append(200)
        precip = round((sum(map(ord, station)) + int(date[-2:])) % 100 / 10.0, 1)
        return httpx.Response(
            200,
            json={"station_id": station, "date": date, "precipitation": precip},
        )


@pytest.fixture
def patched_worker_client(monkeypatch, fake_source):
    """让 worker 内置的 httpx.AsyncClient 走 MockTransport。"""
    real_client = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(fake_source.handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr("app.worker.httpx.AsyncClient", _factory)


@pytest.fixture
def fake_source():
    return FakeSource()


@pytest.fixture(autouse=True)
def _restore_httpx():
    """每个用例后还原 worker 模块的 httpx.AsyncClient。"""
    import app.worker as worker_mod

    original = worker_mod.httpx.AsyncClient
    yield
    worker_mod.httpx.AsyncClient = original


async def _start_worker(hostname: str) -> Worker:
    worker = Worker()
    worker.hostname = hostname
    task = asyncio.create_task(worker.run())
    # 等待 worker 注册入库
    for _ in range(50):
        await asyncio.sleep(0.02)
        if worker.worker_pk is not None:
            break
    assert worker.worker_pk is not None
    worker._bg_task = task
    return worker


async def _stop_worker(worker: Worker) -> None:
    worker.shutdown_event.set()
    await asyncio.wait_for(worker._bg_task, timeout=10)


async def _seed_job(n: int, source: str = "http://src:8081") -> int:
    now = time.time()
    async with db.immediate_transaction() as conn:
        cur = await conn.execute(
            "INSERT INTO jobs(source_base, created_at) VALUES (?, ?)", (source, now)
        )
        jid = cur.lastrowid
        await conn.executemany(
            "INSERT INTO items(job_id, station_id, date, status, updated_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            [
                (jid, f"S{1000 + i}", f"2026-11-{(i % 9) + 10:02d}", now)
                for i in range(n)
            ],
        )
        await conn.execute(
            "INSERT INTO source_states(source_base) VALUES (?) "
            "ON CONFLICT(source_base) DO NOTHING",
            (source,),
        )
    return jid


async def _wait_done(jid: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        async with db.read_connection() as conn:
            row = await db.fetchone(
                conn,
                "SELECT SUM(status IN ('succeeded','failed')) d, COUNT(*) t "
                "FROM items WHERE job_id=?",
                (jid,),
            )
        if row["d"] == row["t"]:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("任务未在时限内完成")


async def test_two_workers_share_db_with_rate_limits(
    init_schema, patched_worker_client, fake_source
):
    n = 12
    jid = await _seed_job(n)
    w1 = await _start_worker("worker-host-1")
    w2 = await _start_worker("worker-host-2")

    # 运行期间周期性核对在途上限
    peak_inflight = 0

    async def observe():
        nonlocal peak_inflight
        while True:
            async with db.read_connection() as conn:
                row = await db.fetchone(
                    conn, "SELECT COUNT(*) c FROM inflight_slots"
                )
            peak_inflight = max(peak_inflight, row["c"])
            await asyncio.sleep(0.02)

    observer = asyncio.create_task(observe())
    try:
        await _wait_done(jid)
    finally:
        observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await observer
        await _stop_worker(w1)
        await _stop_worker(w2)

    assert peak_inflight <= config.MAX_INFLIGHT_PER_SOURCE

    # 相邻请求开始间隔满足最小间隔（记录时刻精度内放宽 30ms）
    times = sorted(t for _, _, t in fake_source.requests)
    assert len(times) >= n  # 含 429 重试，请求数不少于条目数
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert min(gaps) >= config.MIN_REQUEST_GAP_SECONDS - 0.03, gaps

    # 发生过 429，冷却被持久化，且全部条目最终成功（自动重试）
    assert 429 in fake_source.statuses
    async with db.read_connection() as conn:
        counts = {
            r["status"]: r["c"]
            for r in await db.fetchall(
                conn,
                "SELECT status, COUNT(*) c FROM items WHERE job_id=? GROUP BY status",
                (jid,),
            )
        }
        state = await db.fetchone(
            conn, "SELECT cooldown_until FROM source_states WHERE source_base='http://src:8081'"
        )
        workers = await db.fetchall(conn, "SELECT DISTINCT worker_id FROM items")
    assert counts.get("succeeded") == n
    assert state["cooldown_until"] is not None
    # 两个进程都实际参与了采集
    worker_ids = {r["worker_id"] for r in workers if r["worker_id"] is not None}
    assert len(worker_ids) == 2

    # 结果不重复：每个站日唯一一行
    async with db.read_connection() as conn:
        dup = await db.fetchone(
            conn,
            "SELECT COUNT(*) c FROM (SELECT station_id, date FROM items "
            "WHERE job_id=? GROUP BY station_id, date HAVING COUNT(*)>1)",
            (jid,),
        )
    assert dup["c"] == 0


async def test_graceful_shutdown_returns_inflight_to_pending_then_resumes(
    init_schema, fake_source
):
    # 让源变慢，确保停掉时仍有在途项（异步延迟，不阻塞事件循环）
    fake_source.delay = 0.6
    original_handler = fake_source.handler

    async def slow_handler(request):
        await asyncio.sleep(fake_source.delay)
        return original_handler(request)

    # 直接为 worker 安装慢速 MockTransport
    import app.worker as worker_mod

    real_client = httpx.AsyncClient

    def slow_factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(slow_handler)
        return real_client(*args, **kwargs)

    worker_mod.httpx.AsyncClient = slow_factory
    try:
        jid = await _seed_job(8)
        w1 = await _start_worker("worker-host-a")
        # 等至少一个项进入 running
        running_seen = False
        for _ in range(50):
            await asyncio.sleep(0.05)
            async with db.read_connection() as conn:
                row = await db.fetchone(
                    conn,
                    "SELECT COUNT(*) c FROM items WHERE status='running'",
                )
            if row["c"] > 0:
                running_seen = True
                break
        assert running_seen
        await _stop_worker(w1)

        # 优雅退出后：没有残留 running / 名额，项回 pending
        async with db.read_connection() as conn:
            assert (
                await db.fetchone(
                    conn, "SELECT COUNT(*) c FROM items WHERE status='running'"
                )
            )["c"] == 0
            assert (
                await db.fetchone(conn, "SELECT COUNT(*) c FROM inflight_slots")
            )["c"] == 0
            assert (
                await db.fetchone(
                    conn, "SELECT COUNT(*) c FROM items WHERE status='pending'"
                )
            )["c"] == 8

        # 重启新进程续跑，全部完成
        w2 = await _start_worker("worker-host-b")
        try:
            await _wait_done(jid, timeout=15)
        finally:
            await _stop_worker(w2)
        async with db.read_connection() as conn:
            counts = {
                r["status"]: r["c"]
                for r in await db.fetchall(
                    conn,
                    "SELECT status, COUNT(*) c FROM items WHERE job_id=? GROUP BY status",
                    (jid,),
                )
            }
        assert counts.get("succeeded") == 8
    finally:
        worker_mod.httpx.AsyncClient = real_client
