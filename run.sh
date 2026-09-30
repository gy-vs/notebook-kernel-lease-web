#!/bin/bash
# 启动本地服务（生产模式：后端托管已构建的前端）
set -e
cd "$(dirname "$0")"

if [ ! -d frontend_dist ]; then
  echo "前端尚未构建，先执行 cd frontend && npm install && npm run build"
  exit 1
fi

exec .venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port 8765
