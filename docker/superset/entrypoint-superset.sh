#!/usr/bin/env bash
# Initialize Superset, register Trino DB + 4 charts + dashboard, then start
# the webserver on 0.0.0.0:8088.
set -euo pipefail

# Wait for Trino.
for i in {1..60}; do
  if curl -sf http://trino-coordinator:8080/v1/info >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

export PYTHONPATH=/app/docker

echo "[superset] upgrading DB"
superset db upgrade
superset db upgrade
superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@local --password admin \
  || true

echo "[superset] init roles + theme"
superset init

# Start webserver in background, then register DB/charts/dashboard.
echo "[superset] starting webserver in background"
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger &
SUPERSET_PID=$!

# Wait for webserver ready.
for i in {1..60}; do
  if curl -sf http://localhost:8088/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[superset] registering Trino DB + 4 charts + dashboard"
bash /app/docker/bootstrap_superset.sh || echo "[superset] bootstrap failed (continuing)"

# Keep webserver in foreground.
wait $SUPERSET_PID
