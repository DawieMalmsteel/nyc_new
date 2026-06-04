"""DAG: nyc_analytics_refresh

Refresh analytics layer (assumes Spark streaming already running):
  1. dbt build (rebuild views + tests).
  2. Superset bootstrap (refresh dashboard).
  3. Analytics SQL validation.

Schedule: manual trigger; set schedule="@hourly" in production.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nyc",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=15),
}

# Absolute host path (bind mount into airflow container as /repo).
# We need to use the host path because Docker-in-Docker resolves the
# bind-mount source on the host filesystem, not inside the airflow container.
REPO_HOST = "/home/dwcks/vsf_gsm/nyc_new"
REPO_INSIDE_AIRFLOW = Path("/repo")


def _run(cmd: list[str]) -> None:
    log.info("exec: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        log.info("stdout: %s", result.stdout)
    if result.stderr:
        log.error("stderr: %s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"command failed (rc={result.returncode}): {' '.join(cmd)}")


def run_dbt() -> None:
    _run([
        "docker", "run", "--rm",
        "--network", "nyc_new_default",
        "-v", f"{REPO_HOST}:/opt/project",
        "-v", "/var/run/docker.sock:/var/run/docker.sock",
        "nyc-dbt:latest", "entrypoint-dbt",
    ])


def run_superset_bootstrap() -> None:
    _run([
        "docker", "exec", "nyc_superset",
        "bash", "/app/docker/bootstrap_superset.sh",
    ])


def validate_analytics() -> None:
    result = subprocess.run(
        [sys.executable, str(REPO_INSIDE_AIRFLOW / "scripts" / "run_analytics_questions.py")],
        cwd=REPO_INSIDE_AIRFLOW, capture_output=True, text=True,
    )
    log.info("stdout: %s", result.stdout)
    if result.returncode != 0:
        log.error("stderr: %s", result.stderr)
        raise RuntimeError(f"analytics failed:\n{result.stderr}")


with DAG(
    dag_id="nyc_analytics_refresh",
    description="Refresh: dbt + Superset + analytics validation",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "analytics"],
) as dag:
    dbt_build = PythonOperator(task_id="dbt_build", python_callable=run_dbt)
    superset_bootstrap = PythonOperator(task_id="superset_bootstrap", python_callable=run_superset_bootstrap)
    analytics_check = PythonOperator(task_id="analytics_check", python_callable=validate_analytics)

    dbt_build >> superset_bootstrap >> analytics_check
