#!/usr/bin/env bash
# 端到端演示脚本（容器内/本机均可运行，要求 curl 可用）。
# 用法: BASE=http://localhost:8080 SRC=http://localhost:8081 bash scripts/demo.sh
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
SRC="${SRC:-http://localhost:8081}"
# 任务中写入库的源基址：容器内用 compose 服务名，本机直连用 SRC。
JOB_SOURCE="${JOB_SOURCE:-http://source:8081}"

echo "== 1. 气象源切换为每 3 次请求返回一次 429（Retry-After=3 秒） =="
curl -s -X POST "$SRC/control/policy" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"nth429","n":3,"retry_after":3}' | head -c 300; echo

echo "== 2. 提交 12 组站日（同源，最多 20 组） =="
ITEMS=$(JOB_SOURCE="$JOB_SOURCE" python3 - <<'PY'
import json, os
print(json.dumps({
    "source_base": os.environ["JOB_SOURCE"],
    "items": [
        {"station_id": f"S{1000 + i % 4}", "date": f"2026-11-{(i % 9) + 10:02d}"}
        for i in range(12)
    ],
}))
PY
)
JOB=$(curl -s -X POST "$BASE/jobs" -H 'Content-Type: application/json' -d "$ITEMS")
echo "$JOB"
JOB_ID=$(printf '%s' "$JOB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')

echo "== 3. 轮询进度 =="
for _ in $(seq 1 30); do
  P=$(curl -s "$BASE/jobs/$JOB_ID/progress")
  echo "$P"
  if printf '%s' "$P" | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d["done"] else 1)'; then
    break
  fi
  sleep 1
done

echo "== 4. 站日降水结果 =="
curl -s "$BASE/jobs/$JOB_ID/results" | python3 -m json.tool

echo "== 5. 失败项（本场景应为空，429 均自动重试成功） =="
curl -s "$BASE/jobs/$JOB_ID/errors" | python3 -m json.tool

echo "== 6. 源限速状态（冷却/在途/worker 心跳） =="
curl -s "$BASE/sources" | python3 -m json.tool

echo "== 7. 气象源请求时刻核对：最小相邻间隔与在途峰值 =="
curl -s "$SRC/control/stats" | python3 -m json.tool
