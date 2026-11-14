#!/usr/bin/env python3
"""补采提交“取消清理”可复现演示（自包含，使用独立临时库）。

通过 ASGI 传输直接驱动 API，确定性地演示：
1. 提交在“任务行已写、明细未写”时收到 http.disconnect —— 整批撤销，无半批；
2. 等待 SQLite 写锁期间断开 —— 短周期内回滚，解锁后无迟到写入；
3. 已完整提交的任务不受另一个请求取消的影响；
4. 取消后无需重启即可再次提交。

容器内运行：
    docker compose run --rm -e COLLECTOR_TEST_HOOKS=1 api python scripts/cancel_demo.py
本脚本使用 /tmp 下独立数据库，不触碰共享卷中的 collector.db。
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

# 允许从任意工作目录运行：把仓库根目录加入 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# 强制独立临时库，必须在导入 app 之前设置
_tmp = tempfile.mkdtemp(prefix="cancel-demo-")
os.environ["DATA_DIR"] = _tmp
os.environ["COLLECTOR_DB"] = os.path.join(_tmp, "demo.db")
os.environ["COLLECTOR_TEST_HOOKS"] = "1"
os.environ.setdefault("LOCK_ATTEMPT_TIMEOUT_MS", "50")

import httpx  # noqa: E402

from app import db  # noqa: E402
from app.api import app  # noqa: E402
from app.hooks import hooks  # noqa: E402


def _payload(n: int, prefix: str) -> dict:
    return {
        "source_base": "http://src:8081",
        "items": [
            {"station_id": f"{prefix}{1000 + i}", "date": f"2026-11-{(i % 9) + 10:02d}"}
            for i in range(n)
        ],
    }


class ASGICall:
    def __init__(self, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self._incoming: asyncio.Queue = asyncio.Queue()
        self._incoming.put_nowait(
            {"type": "http.request", "body": body, "more_body": False}
        )
        self.sent: list[dict] = []

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
        # 断开导致处理协程被取消是预期行为（真实服务器不会把它当错误响应）
        try:
            await app(scope, self.receive, self.send)
        except asyncio.CancelledError:
            pass


async def _counts() -> tuple[int, int]:
    async with db.read_connection() as conn:
        jobs = (await db.fetchone(conn, "SELECT COUNT(*) c FROM jobs"))["c"]
        items = (await db.fetchone(conn, "SELECT COUNT(*) c FROM items"))["c"]
    return jobs, items


async def main() -> None:
    async with db.write_connection() as conn:
        await db.init_db(conn)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://x") as client:
        # 场景 1：提交中途断开
        print("== 1. 提交中途（任务行已写、明细未写）收到断开 ==")
        call = ASGICall(_payload(4, "A"))
        task = asyncio.create_task(call.run())
        await hooks.wait("after_job_insert")
        call.disconnect()
        await asyncio.wait_for(task, timeout=5)
        jobs, items = await _counts()
        print(f"   断开后 jobs={jobs} items={items}（均应为 0，无半批）")
        assert jobs == 0 and items == 0
        hooks.reset()

        # 场景 2：等锁中断开
        print("== 2. 等待 SQLite 写锁期间断开 ==")
        import sqlite3

        holder = sqlite3.connect(os.environ["COLLECTOR_DB"], isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")
        call2 = ASGICall(_payload(2, "L"))
        task2 = asyncio.create_task(call2.run())
        await asyncio.sleep(0.3)
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        call2.disconnect()
        await asyncio.wait_for(task2, timeout=5)
        elapsed = loop.time() - t0
        holder.execute("COMMIT")
        holder.close()
        await asyncio.sleep(0.3)
        jobs, items = await _counts()
        print(f"   {elapsed*1000:.0f}ms 内回滚完成；解锁后 jobs={jobs} items={items}（应均为 0，无迟到写入）")
        assert jobs == 0 and items == 0 and elapsed < 1.0

        # 场景 3：完整提交保留 + 场景 4：无需重启再提交
        print("== 3. 完整提交的任务不受影响，且无需重启可再提交 ==")
        r = await client.post("/jobs", json=_payload(3, "K"))
        print(f"   首次完整提交: {r.status_code} {r.json()}")
        assert r.status_code == 201
        r = await client.post("/jobs", json=_payload(2, "M"))
        print(f"   取消之后再提交: {r.status_code} {r.json()}")
        assert r.status_code == 201
        jobs, items = await _counts()
        print(f"   最终 jobs={jobs}（应 2） items={items}（应 5），均为整批")
        assert jobs == 2 and items == 5

    print("\n全部取消清理语义验证通过 ✔")


if __name__ == "__main__":
    asyncio.run(main())
