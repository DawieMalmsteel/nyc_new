#!/usr/bin/env bash
# Custom Airflow entrypoint: pick role from AIRFLOW_ROLE env.
# Runs as root (AIRFLOW_UID=0 in compose env), which is fine for local dev.
set -uo pipefail

# Ensure /opt/airflow subdirs exist and are writable.
for d in /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins \
         /opt/airflow/logs/scheduler /opt/airflow/logs/dag_processor_manager \
         /opt/airflow/logs/celery /opt/airflow/logs/dag_processor \
         /opt/airflow/logs/webserver; do
  mkdir -p "$d" 2>/dev/null || true
  chmod a+w "$d" 2>/dev/null || true
done

case "${AIRFLOW_ROLE:-webserver}" in
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  init)
    airflow db migrate
    airflow users create \
      --username admin --firstname Admin --lastname User \
      --email admin@local --password admin --role Admin \
      || true
    echo "[airflow-init] complete"
    ;;
  *)
    exec "$@"
    ;;
esac
