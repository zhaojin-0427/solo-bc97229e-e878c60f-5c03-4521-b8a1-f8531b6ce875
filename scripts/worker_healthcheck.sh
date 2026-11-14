#!/bin/sh
# 采集进程容器健康检查：确认本容器（按 HOSTNAME）注册的 worker 心跳新鲜。
# 以只读 URI 打开 SQLite，绝不干扰采集写事务。
python - <<'PY'
import os, sqlite3, time

db = os.environ.get("COLLECTOR_DB", "/data/collector.db")
host = os.environ.get("HOSTNAME", "")
lease = float(os.environ.get("LEASE_SECONDS", "15"))
try:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=2)
    row = con.execute(
        "SELECT last_heartbeat, active FROM workers WHERE hostname=? "
        "ORDER BY id DESC LIMIT 1", (host,)
    ).fetchone()
    con.close()
except sqlite3.OperationalError:
    raise SystemExit(1)
if not row:
    raise SystemExit(1)
last_heartbeat, active = row
if not active or time.time() - last_heartbeat > lease * 2:
    raise SystemExit(1)
raise SystemExit(0)
PY
