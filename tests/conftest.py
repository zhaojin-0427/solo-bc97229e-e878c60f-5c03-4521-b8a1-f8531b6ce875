"""回归测试公共夹具。

所有测试在临时目录中跑独立的 SQLite 库，并开启时序钩子
（``COLLECTOR_TEST_HOOKS=1``），生产默认关闭、零影响。
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest

# 必须在导入 app.* 之前设置环境变量，config 在导入时读取。
# 测试始终使用独立临时库，绝不触碰 docker 卷中的生产 collector.db。
_TMP = Path(os.environ.get("TMPDIR", "/tmp")) / "collector-tests"
_TMP.mkdir(parents=True, exist_ok=True)
_DB_DIR = _TMP / "db"
_DB_DIR.mkdir(parents=True, exist_ok=True)
# 硬覆盖，避免继承容器里指向共享卷的 COLLECTOR_DB
os.environ["DATA_DIR"] = str(_DB_DIR)
os.environ["COLLECTOR_DB"] = str(_DB_DIR / "collector.db")
os.environ["SOURCE_DB"] = str(_DB_DIR / "source.db")
os.environ["COLLECTOR_TEST_HOOKS"] = "1"
os.environ.setdefault("BUSY_TIMEOUT_MS", "5000")
os.environ.setdefault("LOCK_ATTEMPT_TIMEOUT_MS", "50")
os.environ.setdefault("MIN_REQUEST_GAP_SECONDS", "0.1")
os.environ.setdefault("POLL_INTERVAL_SECONDS", "0.05")
os.environ.setdefault("HEARTBEAT_INTERVAL_SECONDS", "0.2")
os.environ.setdefault("LEASE_SECONDS", "2")
os.environ.setdefault("WORKER_CONCURRENCY", "3")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import db  # noqa: E402
from app.api import app as api_app  # noqa: E402
from app.hooks import hooks  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_db():
    """每个用例使用全新库文件，避免相互污染。"""
    db_path = Path(os.environ["COLLECTOR_DB"])
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        p.unlink(missing_ok=True)
    hooks.reset()
    yield
    hooks.reset()


@pytest.fixture
async def init_schema():
    async with db.write_connection() as conn:
        await db.init_db(conn)


@pytest.fixture
def api():
    return api_app


@pytest.fixture
async def client(init_schema):
    import httpx

    transport = httpx.ASGITransport(app=api_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://collector"
    ) as ac:
        yield ac


@pytest.fixture
def make_payload():
    def _make(n: int = 3, source: str = "http://src:8081", prefix: str = "S"):
        return {
            "source_base": source,
            "items": [
                {"station_id": f"{prefix}{1000 + i}", "date": f"2026-11-{(i % 9) + 10:02d}"}
                for i in range(n)
            ],
        }

    return _make


class WriteLockHolder:
    """在独立同步连接上持有 SQLite 写锁，模拟另一进程的长写入。"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None

    def acquire(self) -> None:
        self.conn = sqlite3.connect(self.db_path, isolation_level=None)
        self.conn.execute("BEGIN IMMEDIATE")

    def release(self) -> None:
        if self.conn is not None:
            self.conn.execute("COMMIT")
            self.conn.close()
            self.conn = None


@pytest.fixture
def lock_holder():
    holder = WriteLockHolder(os.environ["COLLECTOR_DB"])
    yield holder
    holder.release()
