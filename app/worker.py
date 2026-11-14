"""采集进程。

关键约束（两进程共用同一 SQLite 库）：
1. 同源全局在途请求 <= 2；同一源相邻请求“开始”时刻至少间隔 1 秒。
2. 收到 429：以“收到响应的时刻”为起点，按 Retry-After 整数秒设置该源冷却；
   后续任何响应只会延长、不会缩短冷却；冷却期间其他源照常采集。
3. 429 项自动重试（回 pending）；其他 HTTP 错误或字段不符 -> 该项失败。
4. 等待网络 / 冷却期间不持有任何数据库写事务，查询始终可响应。
5. 进程退出：在途项回 pending、释放名额；重启后自动接续，成功项不重复保存。
6. 只回收心跳过期进程的名额，绝不动存活进程；冷却落在数据库里，重启不绕过。
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time

import aiosqlite
import httpx

from . import config, db


class Worker:
    def __init__(self) -> None:
        self.worker_pk: int | None = None
        self.hostname = os.getenv("HOSTNAME", f"worker-{os.getpid()}")
        self.conn: aiosqlite.Connection | None = None
        self.client: httpx.AsyncClient | None = None
        self.write_lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(config.WORKER_CONCURRENCY)
        self.tasks: set[asyncio.Task] = set()
        self.shutdown_event = asyncio.Event()
        # 本进程的内存提示，减少无效轮询；数据库状态才是唯一权威
        self.mem_cooldown: dict[str, float] = {}
        self.mem_gap_hint: dict[str, float] = {}

    # ------------------------------------------------------------------ 生命周期
    async def run(self) -> None:
        self.conn = await db.connect()
        await db.init_db(self.conn)
        now = time.time()
        cur = await self.conn.execute(
            "INSERT INTO workers(hostname, started_at, last_heartbeat, active) "
            "VALUES (?, ?, ?, 1)",
            (self.hostname, now, now),
        )
        self.worker_pk = cur.lastrowid

        self.client = httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.shutdown_event.set)

        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._main_loop()
        finally:
            self.shutdown_event.set()
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._shutdown()
            await self.client.aclose()
            await self.conn.close()

    async def _main_loop(self) -> None:
        while not self.shutdown_event.is_set():
            if len(self.tasks) < config.WORKER_CONCURRENCY:
                launched = await self._spawn_work()
                if not launched:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            self.shutdown_event.wait(),
                            timeout=config.POLL_INTERVAL_SECONDS,
                        )
            else:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.shutdown_event.wait(),
                        timeout=config.POLL_INTERVAL_SECONDS,
                    )
            # 回收已结束的任务
            done = {t for t in self.tasks if t.done()}
            if done:
                self.tasks -= done
                for t in done:
                    exc = t.exception()
                    if exc:
                        print(f"[{self.hostname}] task error: {exc!r}", flush=True)

    async def _spawn_work(self) -> bool:
        """每个轮询周期为每个“当前可发起请求”的源至多启动一个请求。"""
        assert self.conn is not None
        now = time.time()
        async with self.write_lock:
            rows = await db.fetchall(self.conn, 
                "SELECT source_base, last_started_at, cooldown_until FROM source_states"
            )
            sources = [r["source_base"] for r in rows]
            # 还没有状态行的源（新提交的任务）也需要考虑
            job_rows = await db.fetchall(self.conn, 
                "SELECT DISTINCT j.source_base AS sb FROM jobs j "
                "JOIN items i ON i.job_id = j.id WHERE i.status = 'pending'"
            )
            for r in job_rows:
                if r["sb"] not in sources:
                    sources.append(r["sb"])

        launched = False
        for source in sources:
            if len(self.tasks) >= config.WORKER_CONCURRENCY:
                break
            state = await self._acquire(source)
            if state is None:
                continue
            task = asyncio.create_task(self._process_item(source, *state))
            self.tasks.add(task)
            launched = True
        return launched

    # ------------------------------------------------------------- 名额/限速获取
    async def _acquire(self, source: str):
        """原子地检查冷却/在途/1秒间隔并认领一个 pending 项。

        返回 (item_id, station_id, date, request_started_at)；不可发起返回 None。
        整个判定在一个 BEGIN IMMEDIATE 短事务内完成，跨进程串行化。
        注意：事务的每个出口都必须 COMMIT 或 ROLLBACK，绝不能在事务内裸 return。
        """
        assert self.conn is not None
        now = time.time()
        hint = self.mem_cooldown.get(source)
        if hint and hint > now:
            return None
        gap_hint = self.mem_gap_hint.get(source)
        if gap_hint and gap_hint > now:
            return None

        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return None
            try:
                row = await db.fetchone(self.conn,
                    "SELECT last_started_at, cooldown_until FROM source_states "
                    "WHERE source_base = ?",
                    (source,),
                )
                last_started = row["last_started_at"] if row else None
                cooldown_until = row["cooldown_until"] if row else None

                reason: str | None = None
                if cooldown_until and cooldown_until > now:
                    self.mem_cooldown[source] = cooldown_until
                    reason = "cooldown"
                else:
                    inflight = await db.fetchone(self.conn,
                        "SELECT COUNT(*) AS c FROM inflight_slots WHERE source_base = ?",
                        (source,),
                    )
                    if inflight["c"] >= config.MAX_INFLIGHT_PER_SOURCE:
                        reason = "inflight_full"
                    elif (
                        last_started is not None
                        and now - last_started < config.MIN_REQUEST_GAP_SECONDS
                    ):
                        self.mem_gap_hint[source] = (
                            last_started + config.MIN_REQUEST_GAP_SECONDS
                        )
                        reason = "gap"

                item = None
                if reason is None:
                    item = await db.fetchone(self.conn,
                        "SELECT i.id, i.station_id, i.date FROM items i "
                        "JOIN jobs j ON j.id = i.job_id "
                        "WHERE j.source_base = ? AND i.status = 'pending' "
                        "ORDER BY i.id LIMIT 1",
                        (source,),
                    )
                    if item is None:
                        reason = "no_pending"

                if item is not None:
                    await self.conn.execute(
                        "UPDATE items SET status='running', worker_id=?, "
                        "attempts=attempts+1, lease_until=?, updated_at=? WHERE id=?",
                        (
                            self.worker_pk,
                            now + config.LEASE_SECONDS,
                            now,
                            item["id"],
                        ),
                    )
                    await self.conn.execute(
                        "INSERT INTO inflight_slots(source_base, item_id, worker_id, started_at) "
                        "VALUES (?, ?, ?, ?)",
                        (source, item["id"], self.worker_pk, now),
                    )
                    await self.conn.execute(
                        "INSERT INTO source_states(source_base, last_started_at, cooldown_until) "
                        "VALUES (?, ?, NULL) "
                        "ON CONFLICT(source_base) DO UPDATE SET last_started_at=excluded.last_started_at",
                        (source, now),
                    )
                # 唯一的事务出口：无论是否认领都提交（只读判定也需结束事务）
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise
        if item is None:
            return None
        return item["id"], item["station_id"], item["date"], now

    # ------------------------------------------------------------------ 单条采集
    async def _process_item(
        self, source: str, item_id: int, station_id: str, date: str, started_at: float
    ) -> None:
        async with self.sem:
            try:
                assert self.client is not None
                url = source.rstrip("/") + "/daily"
                # 等待网络期间不持有任何数据库事务
                resp = await self.client.get(
                    url, params={"station_id": station_id, "date": date}
                )
                received_at = time.time()

                if resp.status_code == 429:
                    retry_after = self._parse_retry_after(resp)
                    await self._on_rate_limited(
                        source, item_id, received_at, retry_after, started_at
                    )
                    return

                if resp.status_code != 200:
                    await self._finish_failed(
                        item_id,
                        f"HTTP_{resp.status_code}",
                        f"上游返回 HTTP {resp.status_code}",
                        started_at,
                    )
                    return

                try:
                    payload = resp.json()
                except Exception as exc:
                    await self._finish_failed(
                        item_id, "BAD_JSON", f"响应不是合法 JSON: {exc}", started_at
                    )
                    return

                error = self._validate(payload, station_id, date)
                if error:
                    await self._finish_failed(
                        item_id, "FIELD_MISMATCH", error, started_at
                    )
                    return

                await self._finish_succeeded(item_id, float(payload["precipitation"]), started_at)
            except httpx.HTTPError as exc:
                await self._finish_failed(item_id, "NETWORK_ERROR", str(exc), started_at)
            except asyncio.CancelledError:
                # 优雅退出路径：shutdown() 会统一回滚，这里不做任何写操作
                raise
            except Exception as exc:  # 防御性兜底，保证 worker 不挂
                await self._finish_failed(item_id, "WORKER_ERROR", repr(exc), started_at)

    @staticmethod
    def _parse_retry_after(resp: httpx.Response) -> int:
        raw = resp.headers.get("Retry-After", "5")
        try:
            value = int(raw)
            return max(0, value)
        except (TypeError, ValueError):
            return 5

    @staticmethod
    def _validate(payload: object, station_id: str, date: str) -> str | None:
        """核对返回站号、日期，并校验非负毫米降水量。"""
        if not isinstance(payload, dict):
            return "响应体不是 JSON 对象"
        if payload.get("station_id") != station_id:
            return f"站号不符: 期望 {station_id}，实际 {payload.get('station_id')!r}"
        if payload.get("date") != date:
            return f"日期不符: 期望 {date}，实际 {payload.get('date')!r}"
        precip = payload.get("precipitation")
        if isinstance(precip, bool) or not isinstance(precip, (int, float)):
            return f"降水量不是数字: {precip!r}"
        if precip < 0:
            return f"降水量为负值: {precip}"
        return None

    # ------------------------------------------------------------- 结果落库（短事务）
    async def _on_rate_limited(
        self,
        source: str,
        item_id: int,
        received_at: float,
        retry_after: int,
        started_at: float,
    ) -> None:
        if self.shutdown_event.is_set():
            return
        new_until = received_at + retry_after
        assert self.conn is not None
        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                # 拿不到写锁就下一轮重试，项保持 running 由租约/退出逻辑兜底
                return
            try:
                row = await db.fetchone(self.conn, 
                    "SELECT cooldown_until FROM source_states WHERE source_base=?",
                    (source,),
                )
                old_until = row["cooldown_until"] if row else None
                # 后续响应不能缩短冷却：只延长
                effective = max(new_until, old_until) if old_until else new_until
                await self.conn.execute(
                    "INSERT INTO source_states(source_base, cooldown_until) "
                    "VALUES (?, ?) "
                    "ON CONFLICT(source_base) DO UPDATE SET "
                    "cooldown_until=excluded.cooldown_until",
                    (source, effective),
                )
                # 429 项自动重试：回 pending，不占名额
                cur = await self.conn.execute(
                    "UPDATE items SET status='pending', worker_id=NULL, "
                    "lease_until=NULL, error_code='HTTP_429', "
                    "error_message=?, updated_at=? WHERE id=? AND status='running'",
                    (
                        f"收到 429，冷却至 {effective:.3f}（Retry-After={retry_after}s）",
                        time.time(),
                        item_id,
                    ),
                )
                if cur.rowcount:
                    await self.conn.execute(
                        "DELETE FROM inflight_slots WHERE item_id=? AND started_at=?",
                        (item_id, started_at),
                    )
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise
        self.mem_cooldown[source] = effective

    async def _finish_succeeded(
        self, item_id: int, precipitation: float, started_at: float
    ) -> None:
        if self.shutdown_event.is_set():
            return
        assert self.conn is not None
        now = time.time()
        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return
            try:
                cur = await self.conn.execute(
                    "UPDATE items SET status='succeeded', precipitation=?, "
                    "error_code=NULL, error_message=NULL, lease_until=NULL, updated_at=? "
                    "WHERE id=? AND status='running'",
                    (precipitation, now, item_id),
                )
                if cur.rowcount:
                    await self.conn.execute(
                        "DELETE FROM inflight_slots WHERE item_id=? AND started_at=?",
                        (item_id, started_at),
                    )
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise

    async def _finish_failed(
        self, item_id: int, code: str, message: str, started_at: float
    ) -> None:
        if self.shutdown_event.is_set():
            return
        assert self.conn is not None
        now = time.time()
        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return
            try:
                cur = await self.conn.execute(
                    "UPDATE items SET status='failed', error_code=?, error_message=?, "
                    "lease_until=NULL, updated_at=? WHERE id=? AND status='running'",
                    (code, message, now, item_id),
                )
                if cur.rowcount:
                    await self.conn.execute(
                        "DELETE FROM inflight_slots WHERE item_id=? AND started_at=?",
                        (item_id, started_at),
                    )
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------- 心跳与死亡回收
    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(config.HEARTBEAT_INTERVAL_SECONDS)
            if self.shutdown_event.is_set():
                return
            await self._heartbeat()

    async def _heartbeat(self) -> None:
        assert self.conn is not None and self.worker_pk is not None
        now = time.time()
        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return
            try:
                # 自己的心跳
                await self.conn.execute(
                    "UPDATE workers SET last_heartbeat=? WHERE id=?",
                    (now, self.worker_pk),
                )
                # 续租自己的在途项（慢请求不会因租约过期被误判）
                await self.conn.execute(
                    "UPDATE items SET lease_until=? WHERE worker_id=? AND status='running'",
                    (now + config.LEASE_SECONDS, self.worker_pk),
                )
                # 只回收“心跳过期”的进程：存活进程的名额一个都不动
                dead = await db.fetchall(self.conn, 
                    "SELECT id FROM workers WHERE active=1 AND last_heartbeat < ?",
                    (now - config.LEASE_SECONDS,),
                )
                for row in dead:
                    dead_id = row["id"]
                    await self.conn.execute(
                        "UPDATE items SET status='pending', worker_id=NULL, "
                        "lease_until=NULL, updated_at=? "
                        "WHERE worker_id=? AND status='running'",
                        (now, dead_id),
                    )
                    await self.conn.execute(
                        "DELETE FROM inflight_slots WHERE worker_id=?", (dead_id,)
                    )
                    await self.conn.execute(
                        "UPDATE workers SET active=0 WHERE id=?", (dead_id,)
                    )
                # 兜底：租约过期且无对应存活 worker 的 running 项（理论上已被上面覆盖）
                await self.conn.execute(
                    "UPDATE items SET status='pending', worker_id=NULL, lease_until=NULL, updated_at=? "
                    "WHERE status='running' AND lease_until < ? "
                    "AND (worker_id IS NULL OR worker_id IN "
                    "(SELECT id FROM workers WHERE active=0))",
                    (now, now),
                )
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ 优雅退出
    async def _shutdown(self) -> None:
        assert self.conn is not None and self.worker_pk is not None
        # 先等在途请求自然结束（它们会自己落库并释放名额）
        if self.tasks:
            _, pending = await asyncio.wait(
                list(self.tasks),
                timeout=config.SHUTDOWN_GRACE_SECONDS,
                return_when=asyncio.ALL_COMPLETED,
            )
            for t in pending:
                t.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*pending, return_exceptions=True)
        now = time.time()
        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return
            try:
                # 仍未结束的项回 pending，重启后接续；已 succeeded/failed 的不受影响
                await self.conn.execute(
                    "UPDATE items SET status='pending', worker_id=NULL, "
                    "lease_until=NULL, updated_at=? "
                    "WHERE worker_id=? AND status='running'",
                    (now, self.worker_pk),
                )
                await self.conn.execute(
                    "DELETE FROM inflight_slots WHERE worker_id=?", (self.worker_pk,)
                )
                await self.conn.execute(
                    "UPDATE workers SET active=0, last_heartbeat=? WHERE id=?",
                    (now, self.worker_pk),
                )
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise


async def main() -> None:
    await Worker().run()


if __name__ == "__main__":
    asyncio.run(main())
