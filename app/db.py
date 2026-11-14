"""SQLite 表结构与连接。

两个采集进程与 API 进程共用同一个库文件：
- WAL + busy_timeout 保证读写并发不互相阻塞；
- 所有写操作走短事务（BEGIN IMMEDIATE），等待网络 / 冷却期间绝不开事务。
"""
from __future__ import annotations

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


async def connect(db_path: str | None = None) -> aiosqlite.Connection:
    path = db_path or str(config.COLLECTOR_DB)
    conn = await aiosqlite.connect(path, isolation_level=None)
    conn.row_factory = aiosqlite.Row
    conn._ce_busy_timeout = True  # 标记，仅用于可读性
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA busy_timeout=10000")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    return conn


async def init_db(conn: aiosqlite.Connection) -> None:
    await conn.executescript(SCHEMA)


async def fetchone(conn: aiosqlite.Connection, sql: str, params: tuple = ()):
    """aiosqlite 的连接对象没有 execute_fetchone，统一走 cursor。"""
    async with conn.execute(sql, params) as cur:
        return await cur.fetchone()


async def fetchall(conn: aiosqlite.Connection, sql: str, params: tuple = ()):
    async with conn.execute(sql, params) as cur:
        return await cur.fetchall()
