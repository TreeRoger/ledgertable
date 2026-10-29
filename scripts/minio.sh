#!/usr/bin/env bash
# Starts a throwaway MinIO server for the S3 tests unless one is already answering.
set -euo pipefail
PORT="${MINIO_PORT:-9000}"
DATA_DIR="${MINIO_DATA_DIR:-$(cd "$(dirname "$0")/.." && pwd)/.minio}"

if curl -fsS "http://127.0.0.1:${PORT}/minio/health/live" >/dev/null 2>&1; then
  exit 0
fi
command -v minio >/dev/null || { echo "minio binary not found; install it or set LEDGER_S3_ENDPOINT" >&2; exit 1; }
mkdir -p "$DATA_DIR"
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  nohup minio server --quiet --address "127.0.0.1:${PORT}" --console-address "127.0.0.1:$((PORT + 1))" "$DATA_DIR/data" \
  >"$DATA_DIR/minio.log" 2>&1 &
echo $! >"$DATA_DIR/minio.pid"
for _ in $(seq 1 50); do
  curl -fsS "http://127.0.0.1:${PORT}/minio/health/live" >/dev/null 2>&1 && exit 0
  sleep 0.2
done
echo "minio did not become healthy; see $DATA_DIR/minio.log" >&2
exit 1
