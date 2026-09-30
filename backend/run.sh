#!/usr/bin/env bash
# Start the backend (serves the built frontend from frontend/dist if present).
set -euo pipefail
cd "$(dirname "$0")"
exec python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8765 \
  --ws-ping-interval 10 --ws-ping-timeout 20
