#!/usr/bin/env bash
# Wait until a Kafka broker accepts TCP connections.
# Usage: wait-kafka <bootstrap-server>
set -euo pipefail

BOOTSTRAP="${1:-kafka:9092}"
HOST="${BOOTSTRAP%%:*}"
PORT="${BOOTSTRAP##*:}"
DEADLINE=$((SECONDS + 120))

echo "[wait-kafka] waiting for ${HOST}:${PORT} (up to 120s)"
while (( SECONDS < DEADLINE )); do
  if (echo > "/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1; then
    echo "[wait-kafka] ${HOST}:${PORT} is reachable"
    exit 0
  fi
  sleep 2
done

echo "[wait-kafka] timeout waiting for ${HOST}:${PORT}" >&2
exit 1
