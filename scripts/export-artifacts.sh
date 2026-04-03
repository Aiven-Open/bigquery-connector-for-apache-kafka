#!/usr/bin/env bash
# Build image and copy bigquery-connector-for-apache-kafka-*.tar (+ optional .zip) to DEST (default: target/docker-artifacts).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
IMAGE="${IMAGE:-bigquery-kafka-connect:local-build}"
DEST="${1:-$ROOT/target/docker-artifacts}"

docker build -f docker/Dockerfile -t "$IMAGE" "$ROOT"
mkdir -p "$DEST"
docker run --rm \
  -v "$DEST:/out" \
  --entrypoint sh \
  "$IMAGE" \
  -c 'set -e; n=0; for f in kcbq-connector/target/bigquery-connector-for-apache-kafka-*.tar; do [ -f "$f" ] && cp "$f" /out/ && n=$((n+1)); done; for f in kcbq-connector/target/bigquery-connector-for-apache-kafka-*.tar.gz; do [ -f "$f" ] && cp "$f" /out/ && n=$((n+1)); done; [ "$n" -gt 0 ] || { echo "No .tar/.tar.gz under kcbq-connector/target/ — build may have failed:"; ls -la kcbq-connector/target/ 2>&1 || true; exit 1; }; for f in kcbq-connector/target/bigquery-connector-for-apache-kafka-*.zip; do [ -f "$f" ] && cp "$f" /out/ || true; done; ls -la /out'
echo "Artifacts: $DEST"
