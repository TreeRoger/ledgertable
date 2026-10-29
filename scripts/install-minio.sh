#!/usr/bin/env bash
# Builds MinIO from source at the commit the tests and benchmarks were run against.
# MinIO stopped publishing community binaries and Docker images in late 2025, so source is the
# only first-party way to get it. Needs Go >= 1.24; installs into $(go env GOPATH)/bin or $GOBIN.
set -euo pipefail
MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a # RELEASE.2025-10-15T17-29-55Z
go install "github.com/minio/minio@${MINIO_COMMIT}"
