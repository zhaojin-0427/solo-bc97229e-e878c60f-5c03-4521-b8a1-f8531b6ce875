FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 采集进程健康检查：本容器注册的 worker 心跳未过期即为存活
COPY scripts/worker_healthcheck.sh /usr/local/bin/worker_healthcheck.sh
RUN chmod +x /usr/local/bin/worker_healthcheck.sh

EXPOSE 8080 8081
