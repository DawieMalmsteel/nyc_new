"""DAG: nyc_e2e_pipeline

End-to-end orchestration of the NYC Taxi pipeline.

Schedule: manual trigger; set schedule="@daily" in production.
"""

from __future__ import annotations

import logging
import os
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
    "execution_timeout": timedelta(minutes=30),
}

# Absolute host path (resolved on host when binding into spawned containers).
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


def run_spark_streaming() -> None:
    env = os.environ.copy()
    env["MAX_EVENTS"] = os.environ.get("MAX_EVENTS", "1000")
    result = subprocess.run(
        [
            "docker", "run", "--rm",
            "--network", "nyc_new_default",
            "-v", f"{REPO_HOST}:/opt/project",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "nyc-pipeline-tools:latest", "entrypoint-generator",
        ],
        capture_output=True, text=True, env=env,
    )
    if result.stdout:
        log.info("stdout: %s", result.stdout)
    if result.stderr:
        log.error("stderr: %s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"spark streaming failed (rc={result.returncode})")


def run_trino_bootstrap() -> None:
    _run([
        "docker", "run", "--rm",
        "--network", "nyc_new_default",
        "-v", f"{REPO_HOST}:/opt/project",
        "-v", "/var/run/docker.sock:/var/run/docker.sock",
        "nyc-pipeline-tools:latest", "entrypoint-trino-bootstrap",
    ])


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
    dag_id="nyc_e2e_pipeline",
    description="NYC Taxi full pipeline: Spark -> Trino -> dbt -> Superset",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "e2e"],
) as dag:
    spark_streaming = PythonOperator(task_id="spark_streaming", python_callable=run_spark_streaming)
    trino_bootstrap = PythonOperator(task_id="trino_bootstrap", python_callable=run_trino_bootstrap)
    dbt_build = PythonOperator(task_id="dbt_build", python_callable=run_dbt)
    superset_bootstrap = PythonOperator(task_id="superset_bootstrap", python_callable=run_superset_bootstrap)
    analytics_check = PythonOperator(task_id="analytics_check", python_callable=validate_analytics)

    # Dynamically run spark batch for months 01-03
    for m in ["01", "02", "03"]:
        spark_batch = PythonOperator(
            task_id=f"spark_batch_{m}",
            python_callable=_run,
            op_args=[["/opt/spark/bin/spark-submit", "--master", "local[*]", "/opt/project/jobs/spark_local_batch.py",
                      "--input", f"/opt/project/data/raw/yellow_taxi/year=2024/month={m}/yellow_tripdata_2024-{m}.parquet",
                      "--lookup", "/opt/project/data/lookup/taxi_zone_lookup.csv",
                      "--silver", "/opt/project/data/silver/trips",
                      "--quarantine", "/opt/project/data/quarantine/invalid_trips"]]
        )
        spark_batch >> trino_bootstrap

    spark_streaming >> trino_bootstrap >> dbt_build >> superset_bootstrap >> analytics_check
