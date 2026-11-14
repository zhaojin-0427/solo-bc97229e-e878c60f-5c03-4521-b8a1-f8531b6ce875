"""本地模拟气象源。

能力：
- GET /daily?station_id=&date=&delay= 可延迟返回逐日降水 JSON；
- 按策略返回 429（Retry-After 为整数秒），用于验证冷却逻辑；
- 记录每个请求的到达时刻、状态码与耗时，供核对限速效果；
- 支持返回字段不符的坏载荷，验证失败标记。
"""
from __future__ import annotations

import asyncio
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import config

# 策略：normal 正常；nth429 每 n 次请求返回一次 429；bad_payload 返回字段不符
DEFAULT_POLICY: dict[str, object] = {
    "mode": "normal",
    "n": 3,
    "retry_after": 3,
    "counter": 0,
}


class Policy(BaseModel):
    mode: Literal["normal", "nth429", "bad_payload"] = "normal"
    n: int = Field(default=3, ge=1, le=1000)
    retry_after: int = Field(default=3, ge=0, le=3600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    app.state.db = sqlite3.connect(
        str(config.SOURCE_DB), check_same_thread=False, isolation_level=None
    )
    app.state.db.row_factory = sqlite3.Row
    app.state.lock = asyncio.Lock()
    app.state.policy = dict(DEFAULT_POLICY)
    app.state.db.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at REAL NOT NULL,
            station_id TEXT,
            date TEXT,
            status_code INTEGER NOT NULL,
            duration_ms REAL NOT NULL
        );
        """
    )
    yield
    app.state.db.close()


app = FastAPI(title="本地模拟气象源", version="1.0.0", lifespan=lifespan)


def _precipitation(station_id: str, date: str) -> float:
    """确定性的伪降水：站点哈希 + 年内日序，非负，保留一位小数。"""
    day_of_year = time.strptime(date, "%Y-%m-%d").tm_yday
    station_num = sum(ord(c) for c in station_id)
    return round(((station_num * 7 + day_of_year * 3) % 1000) / 10.0, 1)


@app.get("/daily")
async def daily(
    station_id: str = Query(min_length=1, max_length=64),
    date: str = Query(min_length=8, max_length=10),
    delay: float = Query(default=0, ge=0, le=30, description="人为延迟秒数"),
):
    received_at = time.time()
    try:
        time.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="date 应为 YYYY-MM-DD")

    policy = app.state.policy
    async with app.state.lock:
        policy["counter"] += 1
        counter = policy["counter"]
        mode = policy["mode"]
        n = policy["n"]
        retry_after = policy["retry_after"]

    if delay:
        # 延迟期间不占锁，不阻塞其他请求，便于观察“在途≤2”
        await asyncio.sleep(delay)

    if mode == "nth429" and counter % n == 0:
        await _log(received_at, station_id, date, 429)
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": str(retry_after)},
            detail={"error": "rate_limited", "retry_after": retry_after},
        )

    await _log(received_at, station_id, date, 200)
    if mode == "bad_payload":
        # 故意返回错误站号，触发采集端 FIELD_MISMATCH
        return {
            "station_id": station_id + "_WRONG",
            "date": date,
            "precipitation": _precipitation(station_id, date),
        }
    return {
        "station_id": station_id,
        "date": date,
        "precipitation": _precipitation(station_id, date),
        "unit": "mm",
    }


async def _log(received_at: float, station_id: str, date: str, status_code: int) -> None:
    duration_ms = (time.time() - received_at) * 1000
    async with app.state.lock:
        app.state.db.execute(
            "INSERT INTO requests(received_at, station_id, date, status_code, duration_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            (received_at, station_id, date, status_code, duration_ms),
        )


@app.get("/control/requests")
async def list_requests(limit: int = Query(default=200, ge=1, le=1000)):
    """读取请求记录（含每次请求到达时刻），用于核对在途数与 1 秒间隔。"""
    rows = app.state.db.execute(
        "SELECT id, received_at, station_id, date, status_code, duration_ms "
        "FROM requests ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


@app.get("/control/stats")
async def stats():
    """汇总：请求总数、状态码分布、最小相邻请求间隔、同时在途峰值。"""
    rows = app.state.db.execute(
        "SELECT received_at, status_code, duration_ms FROM requests ORDER BY id"
    ).fetchall()
    by_status: dict[int, int] = {}
    gaps = []
    prev = None
    # 区间扫描估算同时在途峰值
    events: list[tuple[float, int]] = []
    for r in rows:
        by_status[r["status_code"]] = by_status.get(r["status_code"], 0) + 1
        if prev is not None:
            gaps.append(round(r["received_at"] - prev, 3))
        prev = r["received_at"]
        start = r["received_at"]
        end = start + r["duration_ms"] / 1000.0
        events.append((start, 1))
        events.append((end, -1))
    peak = cur = 0
    for _, delta in sorted(events):
        cur += delta
        peak = max(peak, cur)
    return {
        "total": len(rows),
        "by_status": by_status,
        "min_gap_seconds": min(gaps) if gaps else None,
        "max_concurrent_observed": peak,
    }


@app.post("/control/policy")
async def set_policy(policy: Policy):
    async with app.state.lock:
        app.state.policy.update(policy.model_dump())
        app.state.policy["counter"] = 0
    return app.state.policy


@app.get("/control/policy")
async def get_policy():
    return app.state.policy


@app.post("/control/reset")
async def reset():
    async with app.state.lock:
        app.state.db.execute("DELETE FROM requests")
        app.state.policy = dict(DEFAULT_POLICY)
    return {"status": "reset"}


@app.get("/health")
async def health():
    return {"status": "ok"}
