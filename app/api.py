"""采集 API：提交任务、查询进度/错误/站日结果、查看限速状态。"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import config, db

MAX_ITEMS_PER_JOB = 20


class StationDate(BaseModel):
    station_id: str = Field(min_length=1, max_length=64)
    date: str = Field(min_length=8, max_length=10, description="ISO 日期，如 2026-11-13")


class JobCreate(BaseModel):
    source_base: str = Field(
        min_length=1,
        max_length=512,
        description="气象源基址，如 http://source:8081，会拼接 /daily",
    )
    items: list[StationDate] = Field(min_length=1, max_length=MAX_ITEMS_PER_JOB)


class JobCreated(BaseModel):
    job_id: int
    item_count: int


class Progress(BaseModel):
    job_id: int
    source_base: str
    total: int
    pending: int
    running: int
    succeeded: int
    failed: int
    done: bool


class ItemOut(BaseModel):
    item_id: int
    station_id: str
    date: str
    status: str
    attempts: int
    precipitation: float | None
    error_code: str | None
    error_message: str | None


class ResultOut(BaseModel):
    item_id: int
    station_id: str
    date: str
    precipitation: float


class SourceStateOut(BaseModel):
    source_base: str
    last_started_at: float | None
    cooldown_until: float | None
    inflight: int
    workers: list[dict[str, Any]]


@asynccontextmanager
async def lifespan(app: FastAPI):
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    app.state.conn = await db.connect()
    await db.init_db(app.state.conn)
    yield
    await app.state.conn.close()


app = FastAPI(
    title="站点逐日降水采集 API",
    version="1.0.0",
    description=(
        "提交气象源地址与最多 20 组站号/日期，两个采集进程共享 SQLite 库，"
        "按源限速（同源在途≤2、请求间隔≥1秒、429 冷却）逐项采集并入库。"
    ),
    lifespan=lifespan,
)


def _conn() -> aiosqlite.Connection:
    return app.state.conn


@app.get("/health")
async def health() -> dict[str, str]:
    await _conn().execute("SELECT 1")
    return {"status": "ok"}


@app.post("/jobs", response_model=JobCreated, status_code=201)
async def create_job(body: JobCreate) -> JobCreated:
    source = body.source_base.strip().rstrip("/")
    if not (source.startswith("http://") or source.startswith("https://")):
        raise HTTPException(status_code=422, detail="source_base 必须是 http(s) URL")
    seen: set[tuple[str, str]] = set()
    for it in body.items:
        key = (it.station_id, it.date)
        if key in seen:
            raise HTTPException(
                status_code=422,
                detail=f"同一任务内存在重复站日: {it.station_id}/{it.date}",
            )
        seen.add(key)
        # 日期格式校验
        try:
            time.strptime(it.date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=422, detail=f"日期格式应为 YYYY-MM-DD: {it.date}"
            )

    now = time.time()
    conn = _conn()
    await conn.execute("BEGIN IMMEDIATE")
    try:
        cur = await conn.execute(
            "INSERT INTO jobs(source_base, created_at) VALUES (?, ?)", (source, now)
        )
        job_id = cur.lastrowid
        await conn.executemany(
            "INSERT INTO items(job_id, station_id, date, status, updated_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            [(job_id, it.station_id, it.date, now) for it in body.items],
        )
        await conn.execute(
            "INSERT INTO source_states(source_base) VALUES (?) "
            "ON CONFLICT(source_base) DO NOTHING",
            (source,),
        )
        await conn.execute("COMMIT")
    except Exception:
        await conn.execute("ROLLBACK")
        raise
    return JobCreated(job_id=job_id, item_count=len(body.items))


@app.get("/jobs/{job_id}/progress", response_model=Progress)
async def job_progress(job_id: int) -> Progress:
    conn = _conn()
    job = await db.fetchone(conn, 
        "SELECT id, source_base FROM jobs WHERE id=?", (job_id,)
    )
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    counts = {
        r["status"]: r["c"]
        for r in await db.fetchall(conn, 
            "SELECT status, COUNT(*) AS c FROM items WHERE job_id=? GROUP BY status",
            (job_id,),
        )
    }
    total = sum(counts.values())
    succeeded, failed = counts.get("succeeded", 0), counts.get("failed", 0)
    return Progress(
        job_id=job_id,
        source_base=job["source_base"],
        total=total,
        pending=counts.get("pending", 0),
        running=counts.get("running", 0),
        succeeded=succeeded,
        failed=failed,
        done=(succeeded + failed) == total,
    )


@app.get("/jobs/{job_id}/errors", response_model=list[ItemOut])
async def job_errors(job_id: int) -> list[ItemOut]:
    conn = _conn()
    if not await db.fetchone(conn, "SELECT 1 FROM jobs WHERE id=?", (job_id,)):
        raise HTTPException(status_code=404, detail="任务不存在")
    rows = await db.fetchall(conn, 
        "SELECT id, station_id, date, status, attempts, precipitation, "
        "error_code, error_message FROM items WHERE job_id=? AND status='failed' "
        "ORDER BY id",
        (job_id,),
    )
    return [_item_out(r) for r in rows]


@app.get("/jobs/{job_id}/results", response_model=list[ResultOut])
async def job_results(job_id: int) -> list[ResultOut]:
    """按任务读取已成功的站日降水结果。"""
    conn = _conn()
    if not await db.fetchone(conn, "SELECT 1 FROM jobs WHERE id=?", (job_id,)):
        raise HTTPException(status_code=404, detail="任务不存在")
    rows = await db.fetchall(conn, 
        "SELECT id, station_id, date, precipitation FROM items "
        "WHERE job_id=? AND status='succeeded' ORDER BY id",
        (job_id,),
    )
    return [
        ResultOut(
            item_id=r["id"],
            station_id=r["station_id"],
            date=r["date"],
            precipitation=r["precipitation"],
        )
        for r in rows
    ]


@app.get("/jobs/{job_id}/items", response_model=list[ItemOut])
async def job_items(job_id: int) -> list[ItemOut]:
    """任务全部条目（含 pending/running 状态与最近一次 429 记录），便于演示。"""
    conn = _conn()
    if not await db.fetchone(conn, "SELECT 1 FROM jobs WHERE id=?", (job_id,)):
        raise HTTPException(status_code=404, detail="任务不存在")
    rows = await db.fetchall(conn, 
        "SELECT id, station_id, date, status, attempts, precipitation, "
        "error_code, error_message FROM items WHERE job_id=? ORDER BY id",
        (job_id,),
    )
    return [_item_out(r) for r in rows]


@app.get("/sources", response_model=list[SourceStateOut])
async def source_states() -> list[SourceStateOut]:
    """查看各源的在途数、最近请求开始时刻与剩余冷却，以及采集进程存活情况。"""
    conn = _conn()
    rows = await db.fetchall(conn, 
        "SELECT s.source_base, s.last_started_at, s.cooldown_until, "
        "(SELECT COUNT(*) FROM inflight_slots k WHERE k.source_base=s.source_base) AS inflight "
        "FROM source_states s ORDER BY s.source_base"
    )
    workers = [
        dict(r)
        for r in await db.fetchall(conn, 
            "SELECT id, hostname, started_at, last_heartbeat, active FROM workers ORDER BY id"
        )
    ]
    return [
        SourceStateOut(
            source_base=r["source_base"],
            last_started_at=r["last_started_at"],
            cooldown_until=r["cooldown_until"],
            inflight=r["inflight"],
            workers=workers,
        )
        for r in rows
    ]


def _item_out(r: aiosqlite.Row) -> ItemOut:
    return ItemOut(
        item_id=r["id"],
        station_id=r["station_id"],
        date=r["date"],
        status=r["status"],
        attempts=r["attempts"],
        precipitation=r["precipitation"],
        error_code=r["error_code"],
        error_message=r["error_message"],
    )
