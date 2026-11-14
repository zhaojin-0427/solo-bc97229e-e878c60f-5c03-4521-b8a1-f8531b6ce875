"""ASGI 断连语义回归：直接驱动 http.disconnect，验证服务端主动取消写入。

不依赖 asyncio 任务取消，而是模拟真实 ASGI 服务器（uvicorn）行为：
请求体发送后、响应完成前，向 receive 通道投递 ``http.disconnect``。
这正是生产中客户端提前断开连接时发生的事件，验证：
- 提交前断开：整批任务与明细回滚，不产生半批；
- 等锁中断开：在一个短取锁周期内回滚，无迟到写入；
- 已提交的完整任务不受影响；
- 断开后无需重启即可继续提交。
"""
from __future__ import annotations

import asyncio

from app import db
from app.api import app as api_app
from app.hooks import hooks


class ASGICall:
    """最小 ASGI 调用器：receive 由队列驱动，可在任意时刻投递 disconnect。"""

    def __init__(self, payload: dict) -> None:
        import json

        body = json.dumps(payload).encode()
        self._incoming: asyncio.Queue[dict] = asyncio.Queue()
        self._incoming.put_nowait(
            {"type": "http.request", "body": body, "more_body": False}
        )
        self.sent: list[dict] = []
        self.raised: BaseException | None = None

    async def receive(self) -> dict:
        return await self._incoming.get()

    async def send(self, message: dict) -> None:
        self.sent.append(message)

    def disconnect(self) -> None:
        self._incoming.put_nowait({"type": "http.disconnect"})

    async def run(self) -> None:
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/jobs",
            "raw_path": b"/jobs",
            "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
            "server": ("collector", 8080),
            "client": ("127.0.0.1", 9000),
        }
        try:
            await api_app(scope, self.receive, self.send)
        except BaseException as exc:  # 断开后响应可能不再发送，记录即可
            self.raised = exc


async def _payload(n: int = 3, prefix: str = "S") -> dict:
    return {
        "source_base": "http://src:8081",
        "items": [
            {"station_id": f"{prefix}{1000 + i}", "date": f"2026-11-{(i % 9) + 10:02d}"}
            for i in range(n)
        ],
    }


async def test_asgi_disconnect_before_commit_rolls_back(init_schema):
    call = ASGICall(await _payload(4))
    server_task = asyncio.create_task(call.run())

    # 等写入到达“jobs 行已写、items 未写”，然后模拟客户端断开
    await hooks.wait("after_job_insert")
    call.disconnect()
    await asyncio.wait_for(server_task, timeout=5)

    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 0
    # 没有发送成功响应（提交未完成）
    assert not any(m.get("type") == "http.response.start" for m in call.sent)


async def test_asgi_disconnect_during_lock_wait_rolls_back(
    init_schema, lock_holder
):
    lock_holder.acquire()
    call = ASGICall(await _payload(2, prefix="L"))
    server_task = asyncio.create_task(call.run())

    # 等取锁循环跑起来，然后在持锁期间断开
    await asyncio.sleep(0.3)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    call.disconnect()
    await asyncio.wait_for(server_task, timeout=5)
    assert loop.time() - t0 < 1.0  # 一个短取锁周期内生效

    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 0

    # 解锁后无迟到写入
    lock_holder.release()
    await asyncio.sleep(0.4)
    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 0


async def test_committed_job_survives_another_disconnect(init_schema):
    # 完整提交一个任务（走正常 ASGI 调用，不断开）
    ok_call = ASGICall(await _payload(3, prefix="A"))
    await ok_call.run()
    start = next(m for m in ok_call.sent if m["type"] == "http.response.start")
    assert start["status"] == 201

    # 第二个任务在提交前断开
    hooks.reset()
    call = ASGICall(await _payload(2, prefix="B"))
    server_task = asyncio.create_task(call.run())
    await hooks.wait("after_job_insert")
    call.disconnect()
    await asyncio.wait_for(server_task, timeout=5)

    # 已提交任务完整保留
    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 1
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 3


async def test_resubmit_works_after_asgi_disconnect(init_schema):
    call = ASGICall(await _payload(2, prefix="R"))
    server_task = asyncio.create_task(call.run())
    await hooks.wait("after_job_insert")
    call.disconnect()
    await asyncio.wait_for(server_task, timeout=5)

    # 断开后无需重启，新的完整请求可正常提交
    hooks.reset()
    ok_call = ASGICall(await _payload(2, prefix="R2"))
    await ok_call.run()
    start = next(m for m in ok_call.sent if m["type"] == "http.response.start")
    assert start["status"] == 201
    async with db.read_connection() as conn:
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"] == 1
        assert (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"] == 2
