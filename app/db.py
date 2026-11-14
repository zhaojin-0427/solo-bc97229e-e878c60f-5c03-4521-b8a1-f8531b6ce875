"""SQLite 表结构与连接。

三个进程（API + 两个采集进程）共用同一个库文件：
- WAL + busy_timeout 保证读写并发不互相阻塞，读连接只读已提交快照；
- 所有写操作走短事务（BEGIN IMMEDIATE），等待网络 / 冷却期间绝不开事务；
- **每次写事务使用独立连接**，事务随请求生命周期结束而关闭，请求被取消时
  绝不会在共享连接上遗留打开的事务，也不会出现“事务嵌套”错误；
- 查询走独立的只读连接，任何写事务（含等待 SQLite 写锁期间）都不阻塞查询，
  且只读连接只能读到完整已提交状态，不可能读到半批数据。
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from typing import AsyncIterator

import aiosqlite

from . import config

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source_base  TEXT NOT NULL,
    created_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS items (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER NOT NULL REFERENCES jobs(id),
    station_id    TEXT NOT NULL,
    date          TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed')),
    precipitation REAL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    error_code    TEXT,
    error_message TEXT,
    worker_id     INTEGER,
    lease_until   REAL,
    updated_at    REAL NOT NULL,
    UNIQUE(job_id, station_id, date)
);
CREATE INDEX IF NOT EXISTS idx_items_job_status ON items(job_id, status);
CREATE INDEX IF NOT EXISTS idx_items_pending ON items(status, lease_until);

-- 采集进程注册表：心跳过期即判定死亡（只按时钟判定，绝不误回收存活进程名额）
CREATE TABLE IF NOT EXISTS workers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    hostname   TEXT NOT NULL,
    started_at REAL NOT NULL,
    last_heartbeat REAL NOT NULL,
    active     INTEGER NOT NULL DEFAULT 1
);

-- 在途请求名额：一行 = 一个已开始、尚未结束的源请求
CREATE TABLE IF NOT EXISTS inflight_slots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source_base  TEXT NOT NULL,
    item_id      INTEGER NOT NULL,
    worker_id    INTEGER NOT NULL,
    started_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_slots_source ON inflight_slots(source_base);
CREATE INDEX IF NOT EXISTS idx_slots_worker ON inflight_slots(worker_id);

-- 每个数据源的全局限速/冷却状态（跨进程共享，冷却持久化，重启不绕过）
CREATE TABLE IF NOT EXISTS source_states (
    source_base   TEXT PRIMARY KEY,
    last_started_at REAL,          -- 最近一次请求开始时刻（墙钟）
    cooldown_until REAL            -- 冷却结束时刻（墙钟），NULL 表示无冷却
);
"""


async def connect(
    db_path: str | None = None,
    *,
    readonly: bool = False,
    busy_timeout_ms: int | None = None,
) -> aiosqlite.Connection:
    """打开一个新的 SQLite 连接。

    每个调用方拿到的都是**独立连接**：请求之间、请求与 worker 之间不共享
    事务状态，因此并发提交不会再触发“cannot start a transaction within a
    transaction”，单个请求取消也不会污染其他请求。

    readonly=True 时以只读 URI 打开，天然只能看到完整已提交快照，且永远
    不会与写事务争抢写锁。busy_timeout_ms 可覆盖连接级取锁等待上限。
    """
    path = db_path or str(config.COLLECTOR_DB)
    timeout_ms = config.BUSY_TIMEOUT_MS if busy_timeout_ms is None else busy_timeout_ms
    if readonly:
        conn = await aiosqlite.connect(
            f"file:{path}?mode=ro", uri=True, isolation_level=None
        )
    else:
        conn = await aiosqlite.connect(path, isolation_level=None)
    conn.row_factory = aiosqlite.Row
    await conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    await conn.execute("PRAGMA synchronous=NORMAL")
    if not readonly:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
    return conn


async def init_db(conn: aiosqlite.Connection) -> None:
    await conn.executescript(SCHEMA)


@contextlib.asynccontextmanager
async def read_connection() -> AsyncIterator[aiosqlite.Connection]:
    """查询专用的短连接：只读、独立，写锁等待期间照常工作。"""
    conn = await connect(readonly=True)
    try:
        yield conn
    finally:
        await conn.close()


@contextlib.asynccontextmanager
async def write_connection() -> AsyncIterator[aiosqlite.Connection]:
    """写事务专用的短连接：关闭即释放锁并回滚任何未提交事务。"""
    conn = await connect()
    try:
        yield conn
    finally:
        await conn.close()


@contextlib.asynccontextmanager
async def immediate_transaction() -> AsyncIterator[aiosqlite.Connection]:
    """取消安全的 ``BEGIN IMMEDIATE`` 短事务，每次使用独立连接。

    取锁被拆成“短 busy_timeout 尝试 + 让出事件循环”的循环：

    - 正常路径：拿到写锁后执行整批语句，全部成功才 COMMIT，否则 ROLLBACK；
    - 请求在等待 SQLite 写锁时被取消：最迟在一个
      :data:`config.LOCK_ATTEMPT_TIMEOUT_MS` 周期内收到取消，立即 interrupt
      阻塞中的取锁并关闭连接，绝不干等完整 busy_timeout，绝不遗留打开的
      事务，也不会有“迟到的写入”；
    - 请求在事务执行中途被取消：以不可取消的清理流程完成 ROLLBACK 后再关
      连接，撤销本批全部改动；已完整提交的其他事务完全不受影响；
    - 连接随上下文关闭，其他请求使用各自的连接，清理动作不干扰任何人。
    """
    conn = await connect(busy_timeout_ms=config.LOCK_ATTEMPT_TIMEOUT_MS)
    deadline = time.monotonic() + config.BUSY_TIMEOUT_MS / 1000.0
    started = False
    committed = False
    cancelled = False
    try:
        while True:
            try:
                await conn.execute("BEGIN IMMEDIATE")
                started = True
                break
            except aiosqlite.OperationalError as exc:
                # 仅“锁忙”可重试；其余错误（如磁盘/权限）直接抛出
                if "locked" not in str(exc).lower():
                    raise
                if time.monotonic() >= deadline:
                    raise
                # 让出事件循环：取消信号在此处被及时投递，且不忙等占 CPU
                await asyncio.sleep(config.LOCK_RETRY_INTERVAL_SECONDS)
        yield conn
        await conn.execute("COMMIT")
        committed = True
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        need_rollback = started and not committed
        if cancelled:
            # 只有“取锁等待中（事务尚未开启）”才 interrupt：此时没有需要
            # 保护的事务，interrupt 可立刻打断阻塞的 BEGIN 尝试。
            # 事务已开启时绝不 interrupt——否则可能误伤随后的 ROLLBACK，
            # 导致写锁无法释放。
            if not started:
                with contextlib.suppress(Exception):
                    conn._conn.interrupt()
            # 清理必须真正跑完：被取消任务中的普通 await 会立刻再次抛出
            # CancelledError，因此用不可取消的循环守护回滚与关连接。
            await _run_uncancellable(_finalize(conn, need_rollback))
        else:
            if need_rollback:
                await _rollback(conn)
            await conn.close()


async def _finalize(conn: aiosqlite.Connection, rollback: bool) -> None:
    if rollback:
        await _rollback(conn)
    await conn.close()


async def _run_uncancellable(coro) -> None:
    """等待清理协程真正结束，期间到达的取消不中断清理。"""
    task = asyncio.ensure_future(coro)
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    exc = task.exception()
    if exc is not None:
        raise exc


async def _rollback(conn: aiosqlite.Connection) -> None:
    with contextlib.suppress(aiosqlite.OperationalError, aiosqlite.ProgrammingError):
        await conn.execute("ROLLBACK")


async def fetchone(conn: aiosqlite.Connection, sql: str, params: tuple = ()):
    """aiosqlite 的连接对象没有 execute_fetchone，统一走 cursor。"""
    async with conn.execute(sql, params) as cur:
        return await cur.fetchone()


async def fetchall(conn: aiosqlite.Connection, sql: str, params: tuple = ()):
    async with conn.execute(sql, params) as cur:
        return await cur.fetchall()
