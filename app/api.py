"""采集 API：提交任务、查询进度/错误/站日结果、查看限速状态。

并发与取消语义（本模块修复要点）：
- 每次提交在**独立连接**上跑一个 ``BEGIN IMMEDIATE`` 短事务，整批
  （jobs 行 + 全部 items 行 + source_states 行）原子提交；并发提交各自
  串行拿到写锁后整批成功，互不嵌套、互不污染。
- 请求在提交前（含等待 SQLite 写锁期间或写入中途）被客户端取消：
  事务回滚、连接关闭，本批任务与明细全部撤销，不遗留任何打开的事务，
  也不会在解锁后出现迟到写入；已完整提交的任务不受影响。
- 所有查询走**独立只读连接**，只能读到完整已提交状态：写锁等待期间
  健康检查、进度、结果照常响应，且绝不会把“只有任务行、没有明细”的
  半批数据报为完成。
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from . import config, db
from .hooks import hooks

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
    # 初始化也使用短连接，避免在应用状态里持有共享写连接
    async with db.write_connection() as conn:
        await db.init_db(conn)
    yield


app = FastAPI(
    title="站点逐日降水采集 API",
    version="1.1.0",
    description=(
        "提交气象源地址与最多 20 组站号/日期，两个采集进程共享 SQLite 库，"
        "按源限速（同源在途≤2、请求间隔≥1秒、429 冷却）逐项采集并入库。"
        "提交为整批原子事务，取消可安全回滚；查询只读已提交快照。"
    ),
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    # 独立只读连接：写事务/写锁等待期间仍可响应
    async with db.read_connection() as conn:
        await conn.execute("SELECT 1")
    return {"status": "ok"}


@app.post("/jobs", response_model=JobCreated, status_code=201)
async def create_job(request: Request, body: JobCreate) -> JobCreated:
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

    async def _write_batch() -> JobCreated:
        # 独立连接 + 取消安全事务：客户端在等锁/写入/提交前任意时刻断开，
        # 本批 jobs/items/source_states 改动都会随回滚整体撤销。
        async with db.immediate_transaction() as conn:
            # 测试闸门：此时已拿到写锁、尚未写入（用于控制“写入中/等锁”时序）
            await hooks.gate("after_begin")
            cur = await conn.execute(
                "INSERT INTO jobs(source_base, created_at) VALUES (?, ?)",
                (source, now),
            )
            job_id = cur.lastrowid
            # 测试闸门：jobs 行已写、items 尚未写（用于验证读不到半批、中途取消）
            await hooks.gate("after_job_insert")
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
            # 测试闸门：全部明细已写、尚未提交（用于并发查询与提交时序控制）
            await hooks.gate("before_commit")
        # 离开上下文即 COMMIT 并关闭连接；走到这里说明整批已完整提交。
        return JobCreated(job_id=job_id, item_count=len(body.items))

    # 显式监听客户端断开：HTTP 服务端不会因对端断连自动取消处理协程。
    # 请求体已由 FastAPI 解析完毕，这里只等待 receive 上**后续**的
    # http.disconnect 消息。写入放在独立子任务里与断开事件竞争：提交完成前
    # 客户端一旦断开，立即取消写入任务，由 immediate_transaction 的取消安全
    # 逻辑回滚本批并关闭连接（等锁期间同样在一个短取锁周期内生效）。
    # 不使用 request.is_disconnected()：它会再次驱动 receive，部分测试传输
    # 在响应完成前不产出 disconnect 消息，会造成死等。
    batch_task = asyncio.ensure_future(_write_batch())

    async def _watch_disconnect() -> None:
        message = await request.receive()
        if message.get("type") == "http.disconnect" and not batch_task.done():
            batch_task.cancel()

    disconnect_task = asyncio.ensure_future(_watch_disconnect())
    try:
        return await batch_task
    finally:
        # 写入已结束（成功或失败）：断开监听不再需要，取消它以免悬挂在
        # receive 上；正常响应路径下它本就不会收到 disconnect。
        disconnect_task.cancel()


@app.get("/jobs/{job_id}/progress", response_model=Progress)
async def job_progress(job_id: int) -> Progress:
    async with db.read_connection() as conn:
        job = await db.fetchone(
            conn, "SELECT id, source_base FROM jobs WHERE id=?", (job_id,)
        )
        if job is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        counts = {
            r["status"]: r["c"]
            for r in await db.fetchall(
                conn,
                "SELECT status, COUNT(*) AS c FROM items WHERE job_id=? GROUP BY status",
                (job_id,),
            )
        }
    total = sum(counts.values())
    succeeded, failed = counts.get("succeeded", 0), counts.get("failed", 0)
    # total==0 说明读到了异常的空任务（正常提交为整批原子，不可能出现），
    # 绝不能把它报为完成。
    done = total > 0 and (succeeded + failed) == total
    return Progress(
        job_id=job_id,
        source_base=job["source_base"],
        total=total,
        pending=counts.get("pending", 0),
        running=counts.get("running", 0),
        succeeded=succeeded,
        failed=failed,
        done=done,
    )


@app.get("/jobs/{job_id}/errors", response_model=list[ItemOut])
async def job_errors(job_id: int) -> list[ItemOut]:
    async with db.read_connection() as conn:
        if not await db.fetchone(conn, "SELECT 1 FROM jobs WHERE id=?", (job_id,)):
            raise HTTPException(status_code=404, detail="任务不存在")
        rows = await db.fetchall(
            conn,
            "SELECT id, station_id, date, status, attempts, precipitation, "
            "error_code, error_message FROM items WHERE job_id=? AND status='failed' "
            "ORDER BY id",
            (job_id,),
        )
    return [_item_out(r) for r in rows]


@app.get("/jobs/{job_id}/results", response_model=list[ResultOut])
async def job_results(job_id: int) -> list[ResultOut]:
    """按任务读取已成功的站日降水结果（只读已提交快照）。"""
    async with db.read_connection() as conn:
        if not await db.fetchone(conn, "SELECT 1 FROM jobs WHERE id=?", (job_id,)):
            raise HTTPException(status_code=404, detail="任务不存在")
        rows = await db.fetchall(
            conn,
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
    async with db.read_connection() as conn:
        if not await db.fetchone(conn, "SELECT 1 FROM jobs WHERE id=?", (job_id,)):
            raise HTTPException(status_code=404, detail="任务不存在")
        rows = await db.fetchall(
            conn,
            "SELECT id, station_id, date, status, attempts, precipitation, "
            "error_code, error_message FROM items WHERE job_id=? ORDER BY id",
            (job_id,),
        )
    return [_item_out(r) for r in rows]


@app.get("/sources", response_model=list[SourceStateOut])
async def source_states() -> list[SourceStateOut]:
    """查看各源的在途数、最近请求开始时刻与剩余冷却，以及采集进程存活情况。"""
    async with db.read_connection() as conn:
        rows = await db.fetchall(
            conn,
            "SELECT s.source_base, s.last_started_at, s.cooldown_until, "
            "(SELECT COUNT(*) FROM inflight_slots k WHERE k.source_base=s.source_base) AS inflight "
            "FROM source_states s ORDER BY s.source_base",
        )
        workers = [
            dict(r)
            for r in await db.fetchall(
                conn,
                "SELECT id, hostname, started_at, last_heartbeat, active "
                "FROM workers ORDER BY id",
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


def _item_out(r) -> ItemOut:
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
