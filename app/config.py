"""运行参数，全部可用环境变量覆盖。"""
import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
COLLECTOR_DB = Path(os.getenv("COLLECTOR_DB", str(DATA_DIR / "collector.db")))
SOURCE_DB = Path(os.getenv("SOURCE_DB", str(DATA_DIR / "source.db")))

# 每个数据源允许的最大在途请求数（两个采集进程共用同一库，因此是全局值）
MAX_INFLIGHT_PER_SOURCE = int(os.getenv("MAX_INFLIGHT_PER_SOURCE", "2"))
# 同一数据源相邻两次“请求开始”的最小间隔（秒）
MIN_REQUEST_GAP_SECONDS = float(os.getenv("MIN_REQUEST_GAP_SECONDS", "1"))
# 采集轮询间隔
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0.25"))
# 单个采集进程同时处理的条目上限（在途名额的上限仍由数据源状态表裁决）
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))
# 心跳 / 租约时长
HEARTBEAT_INTERVAL_SECONDS = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "2"))
LEASE_SECONDS = float(os.getenv("LEASE_SECONDS", "15"))
# 优雅退出时等待在途请求的最长时间
SHUTDOWN_GRACE_SECONDS = float(os.getenv("SHUTDOWN_GRACE_SECONDS", "20"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
