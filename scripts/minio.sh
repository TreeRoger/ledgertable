#!/usr/bin/env bash
# Makes sure the S3 endpoint in LEDGER_S3_ENDPOINT answers. A local endpoint that is down gets a
# throwaway MinIO server; a remote one (e.g. the docker compose service) is waited for.
set -euo pipefail
ENDPOINT="${LEDGER_S3_ENDPOINT:-http://127.0.0.1:9000}"
DATA_DIR="${MINIO_DATA_DIR:-$(cd "$(dirname "$0")/.." && pwd)/.minio}"
healthy() { curl -fsS "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; }

if healthy; then
  exit 0
fi
case "$ENDPOINT" in
  http://127.0.0.1:*|http://localhost:*)
    command -v minio >/dev/null || { echo "minio not found; run scripts/install-minio.sh or point LEDGER_S3_ENDPOINT elsewhere" >&2; exit 1; }
    PORT="${ENDPOINT##*:}"
    mkdir -p "$DATA_DIR"
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
      nohup minio server --quiet --address "127.0.0.1:${PORT}" --console-address "127.0.0.1:$((PORT + 1))" \
      "$DATA_DIR/data" >"$DATA_DIR/minio.log" 2>&1 &
    echo $! >"$DATA_DIR/minio.pid"
    ;;
esac
for _ in $(seq 1 100); do
  healthy && exit 0
  sleep 0.2
done
echo "S3 endpoint ${ENDPOINT} is not healthy" >&2
exit 1
