#!/usr/bin/env bash
# Describes the machine benchmarks ran on, for bench/results/machine.txt.
set -euo pipefail
echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "os: $(uname -srm)"
if [[ "$(uname)" == Darwin ]]; then
  echo "cpu: $(sysctl -n machdep.cpu.brand_string) ($(sysctl -n hw.ncpu) cores)"
  echo "memory: $(( $(sysctl -n hw.memsize) / 1073741824 )) GiB"
else
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs) ($(nproc) cores)"
  echo "memory: $(( $(grep MemTotal /proc/meminfo | awk '{print $2}') / 1048576 )) GiB"
fi
echo "java: $(java -version 2>&1 | head -1)"
echo "s3: MinIO at ${LEDGER_S3_ENDPOINT:-unset} ($(minio --version 2>/dev/null | head -1 || echo 'remote'))"
