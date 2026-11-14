# 站点逐日降水批量采集系统

气象数据工程师批量补取站点逐日降水、送入干旱分析库的采集 API。FastAPI + SQLite，
两个采集进程共享同一数据库，在不触发源站封禁的前提下并行采集。

## 本次修复：补采提交的事务隔离与取消清理

旧实现的所有写操作共用 API 进程内的**单个 aiosqlite 连接**，并发提交时一个请求
遗留的未结束事务会让另一个请求撞上 “cannot start a transaction within a
transaction”；任务行已写、明细未写时取消请求会留下半批数据，之后的提交持续 500，
进度查询还把空任务报为完成。修复要点：

| 问题 | 修复方式 |
| --- | --- |
| 并发提交事务嵌套、互相污染 | 每次写事务使用**独立连接**（`db.immediate_transaction`），整批 `jobs + items + source_states` 在一个 `BEGIN IMMEDIATE` 短事务内原子提交 |
| 半批数据被读到、空任务报完成 | 所有查询走**独立只读连接**（`mode=ro`），只能看到完整已提交快照；`done` 增加 `total>0` 守卫，空任务绝不报完成 |
| 提交前/写入中取消 | 取消安全事务：回滚本批并以不可取消的清理关闭连接，任务行与明细整体撤销；已完整提交的任务不受影响 |
| 等待 SQLite 写锁时取消 | 取锁拆成「短 `busy_timeout` 尝试 + 让出事件循环」循环，取消在一个短周期（默认 100ms）内生效；等待中无打开事务，interrupt 后直接关连接，解锁后**无迟到写入、无遗留事务** |
| 客户端断连服务端不感知 | 提交端点显式监听 ASGI `http.disconnect`，提交完成前断连即取消写入子任务 |
| 清理干扰其他请求 / 需重启 | 每请求独立连接，取消只回滚并关闭自己的连接；其他请求与已提交数据完全不受影响，无需重启即可再提交 |
| 写锁等待期间查询不可用 | 只读连接不参与写锁竞争，健康检查、进度、站日结果在写入/等锁期间始终可响应，且取自完整已提交状态 |

原有的双进程采集、限速（同源在途 ≤ 2、请求间隔 ≥ 1 秒）、429 冷却与自动重试、
租约/心跳/死亡回收、优雅退出接续规则全部保留，未做行为变更。

## 限速与可靠性规则（保留）

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
  db.py       SQLite 连接、只读/写短连接、取消安全的 immediate_transaction
  hooks.py    回归测试时序闸门（默认关闭，生产零影响）
  api.py      采集 API（提交/进度/错误/结果/限速状态，断连感知）
  worker.py   采集进程（限速、冷却、租约、优雅退出、死亡回收）
  source.py   本地模拟气象源（可延迟、可发 429、记录请求时刻）
tests/        回归测试（事务隔离/取消清理/并发查询/双进程采集）
scripts/
  demo.sh               端到端演示
  cancel_demo.py        取消清理语义自包含演示（独立临时库）
  worker_healthcheck.sh 容器健康检查
docker-compose.yml      source + api + worker1 + worker2 + tests
```

## 启动

```bash
docker compose up --build
```

- 采集 API 与交互文档（Swagger UI）：http://localhost:8080/docs
- 模拟气象源与控制接口文档：http://localhost:8081/docs
- 两个采集进程 `worker1`、`worker2` 共享卷 `collector-data` 中的 `collector.db`

## 回归测试

测试覆盖并发提交整批成功、查询读不到半批、提交前/等锁中取消的回滚、清理不干扰
其他请求、写锁等待期间查询可响应、空任务不报完成，以及双进程下的限速/冷却/优雅
退出接续。测试使用独立临时库，并通过 `app/hooks.py` 的命名闸门确定性控制
「写入中 / 等锁 / 取消 / 并发查询」时序。

```bash
# 本地
pip install -r requirements.txt
python -m pytest -q

# 容器（一次性服务，独立临时库，不挂载共享卷，跑完即退出）
docker compose run --rm tests
```

取消清理语义的自包含演示（同样使用独立临时库，不影响共享卷）：

```bash
docker compose run --rm -e COLLECTOR_TEST_HOOKS=1 api python scripts/cancel_demo.py
```

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
| `BUSY_TIMEOUT_MS` | 10000 | 取锁总等待上限（毫秒），超时该请求失败且不遗留事务 |
| `LOCK_ATTEMPT_TIMEOUT_MS` | 100 | 单次取锁尝试的 busy_timeout，决定等锁时取消的响应粒度 |
| `LOCK_RETRY_INTERVAL_SECONDS` | 0.02 | 取锁短尝试之间的让出间隔 |
| `COLLECTOR_TEST_HOOKS` | 0 | 置 1 启用回归测试时序闸门，默认关闭、生产零影响 |
