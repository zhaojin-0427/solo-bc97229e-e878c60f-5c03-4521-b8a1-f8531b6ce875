# 站点逐日降水批量采集系统

气象数据工程师批量补取站点逐日降水、送入干旱分析库的采集 API。FastAPI + SQLite，
两个采集进程共享同一数据库，在不触发源站封禁的前提下并行采集。

## 限速与可靠性规则（已全部实现）

| 规则 | 实现方式 |
| --- | --- |
| 同源全局在途请求 ≤ 2（两进程共享） | `inflight_slots` 表计数，认领条目时在 `BEGIN IMMEDIATE` 短事务内原子检查 |
| 同源相邻请求开始间隔 ≥ 1 秒 | `source_states.last_started_at` 墙钟时间戳，跨进程共享判定 |
| 收到 429 后按 `Retry-After` 整数秒冷却，从**收到响应时刻**起算 | `source_states.cooldown_until = received_at + retry_after`，持久化在库中 |
| 后续响应不能缩短冷却 | 新冷却只取 `max(新值, 旧值)`；200 响应不写冷却字段 |
| 冷却只针对该源，其他源继续 | 冷却按 `source_base` 隔离，worker 循环按源逐个尝试 |
| 429 项自动重试 | 条目回 `pending`、释放名额，下一轮重新认领 |
| 其他 HTTP 错误 / 字段不符 → 该项失败 | `failed` + `error_code/error_message`，不重试 |
| 等待网络或冷却不占库写事务 | 仅在认领/落库瞬间开短事务，HTTP 请求与 sleep 期间无事务；SQLite WAL 保证查询始终可响应 |
| 进程退出后自动接续、不重复保存 | 优雅退出把在途项回 `pending` 并删名额；重启后续跑；成功项更新带 `status='running'` 条件，重复结束不覆盖结果 |
| 不误回收存活进程名额 | 只回收心跳超过租约期（15s）的 worker 名下名额；存活 worker 每 2s 心跳并给自己的条目续租 |
| 重启不能绕过剩余冷却 | 冷却结束时刻是库内墙钟绝对值，新进程启动直接读取，无内存重置入口 |
| 降水量校验 | 必须是数字且非负（毫米），并核对返回站号、日期与请求一致 |

## 目录结构

```
app/
  config.py   运行参数（环境变量可覆盖）
  db.py       SQLite 连接与表结构（WAL）
  api.py      采集 API（提交/进度/错误/结果/限速状态）
  worker.py   采集进程（限速、冷却、租约、优雅退出、死亡回收）
  source.py   本地模拟气象源（可延迟、可发 429、记录请求时刻）
scripts/
  demo.sh               端到端演示
  worker_healthcheck.sh 容器健康检查
docker-compose.yml      source + api + worker1 + worker2
```

## 启动

```bash
docker compose up --build
```

- 采集 API 与交互文档：http://localhost:8080/docs
- 模拟气象源与控制接口文档：http://localhost:8081/docs
- 两个采集进程 `worker1`、`worker2` 共享卷 `collector-data` 中的 `collector.db`

## 交互式演示（Swagger UI）

1. 打开气象源文档 http://localhost:8081/docs ，调用 `POST /control/policy`，
    body 填 `{"mode":"nth429","n":3,"retry_after":3}` —— 每 3 个请求返回一次 429。
2. 打开采集 API 文档 http://localhost:8080/docs ，调用 `POST /jobs`：

   ```json
   {
     "source_base": "http://source:8081",
     "items": [
       {"station_id": "S1001", "date": "2026-11-10"},
       {"station_id": "S1001", "date": "2026-11-11"},
       {"station_id": "S1002", "date": "2026-11-10"}
     ]
   }
   ```

   单次最多 20 组站号/日期。
3. 调用 `GET /jobs/{job_id}/progress` 看进度（pending/running/succeeded/failed）。
4. 调用 `GET /jobs/{job_id}/results` 读取站日降水结果；
   `GET /jobs/{job_id}/errors` 看失败项；`GET /jobs/{job_id}/items` 看全部条目，
   429 重试可在 `attempts` 与 `error_message` 中看到痕迹。
5. 调用 `GET /sources` 查看各源在途数、最近请求开始时刻、冷却截止时刻与 worker 心跳。
6. 回到气象源文档调用 `GET /control/stats`：`min_gap_seconds` 应 ≥ 1，
   `max_concurrent_observed` 应 ≤ 2；`GET /control/requests` 可逐条核对请求到达时刻。

也可以直接运行脚本：

```bash
docker compose exec api bash /app/scripts/demo.sh   # 容器内（源用 compose 服务名）
# 或宿主机（容器已映射端口，需把任务里的源地址也指向本机）：
BASE=http://localhost:8080 SRC=http://localhost:8081 \
  JOB_SOURCE=http://localhost:8081 bash scripts/demo.sh
```

## 其他可演示场景

- **延迟响应观察在途上限**：`GET /daily?...&delay=5`，或提交后用
  `GET /sources` 观察 `inflight` 不超过 2。
- **字段不符 → 失败项**：`POST /control/policy` body `{"mode":"bad_payload"}`，
  新任务的条目会标记为 `failed / FIELD_MISMATCH`。
- **普通 HTTP 错误**：把任务的 `source_base` 指向一个返回 500 的地址（或不存在的路径），
  条目标记 `failed / HTTP_500`，不重试。
- **进程退出接续**：`docker compose kill worker1`（SIGKILL 模拟宕机），
  worker2 的心跳回收逻辑会在租约到期后把它的在途项接走；
  `docker compose restart worker1` 后冷却仍在库中继续生效。
  正常 `docker compose stop` 走优雅退出，在途项立即回 `pending`。

## 配置（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `MAX_INFLIGHT_PER_SOURCE` | 2 | 每源全局在途上限 |
| `MIN_REQUEST_GAP_SECONDS` | 1 | 同源相邻请求开始最小间隔 |
| `LEASE_SECONDS` | 15 | 心跳/租约时长，超过即判定进程死亡 |
| `HEARTBEAT_INTERVAL_SECONDS` | 2 | 心跳与续租间隔 |
| `WORKER_CONCURRENCY` | 5 | 单进程并发处理条目数 |
| `HTTP_TIMEOUT_SECONDS` | 20 | 上游请求超时（超时记 `NETWORK_ERROR` 失败） |
| `POLL_INTERVAL_SECONDS` | 0.25 | 空转轮询间隔 |
