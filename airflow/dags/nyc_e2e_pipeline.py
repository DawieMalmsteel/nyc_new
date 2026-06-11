"""DAG: nyc_e2e_pipeline

End-to-end orchestration of the NYC Taxi pipeline.
Dynamic Incremental Design with Native Airflow Catchup.
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

IS_K8S = os.path.exists("/var/run/secrets/kubernetes.io") or "KUBERNETES_SERVICE_HOST" in os.environ


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
    if IS_K8S:
        _run(["kubectl", "delete", "job", "spark-streaming", "-n", "nyc-taxi", "--ignore-not-found"])
        _run(["kubectl", "apply", "-f", "/repo/k8s/jobs/spark-streaming.yaml", "-n", "nyc-taxi"])
        _run(["kubectl", "wait", "--for=condition=complete", "job/spark-streaming", "-n", "nyc-taxi", "--timeout=300s"])
    else:
        # Run local Spark streaming via docker run
        _run([
            "docker", "run", "--rm",
            "--network", "nyc_new_default",
            "-v", f"{REPO_HOST}:/opt/project",
            "-e", "MINIO_ENDPOINT=http://minio:9000",
            "-e", "MINIO_ACCESS_KEY=minio",
            "-e", "MINIO_SECRET_KEY=minio123",
            "apache/spark:3.5.1",
            "/opt/spark/bin/spark-submit",
            "--master", "local[*]",
            "--conf", "spark.jars.ivy=/opt/project/.ivy2",
            "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
            "--conf", "spark.scheduler.mode=FAIR",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "/opt/project/jobs/spark_stream_taxi_events.py",
            "--bootstrap-server", "kafka:9092",
            "--topic", "taxi.trip.events",
            "--lookup-path", "s3a://nyc-lookup/taxi_zone_lookup.csv",
            "--silver-path", "s3a://nyc-silver/trips",
            "--quarantine-path", "s3a://nyc-quarantine/invalid_trips",
            "--checkpoint-path", "s3a://nyc-silver/checkpoints/spark_stream_taxi_events/taxi.trip.events",
            "--trigger-available-now"
        ])


def run_trino_bootstrap() -> None:
    if IS_K8S:
        _run(["kubectl", "delete", "job", "trino-bootstrap", "-n", "nyc-taxi", "--ignore-not-found"])
        _run(["kubectl", "apply", "-f", "/repo/k8s/jobs/trino-bootstrap.yaml", "-n", "nyc-taxi"])
        _run(["kubectl", "wait", "--for=condition=complete", "job/trino-bootstrap", "-n", "nyc-taxi", "--timeout=120s"])
    else:
        _run([
            "docker", "run", "--rm",
            "--network", "nyc_new_default",
            "-v", f"{REPO_HOST}:/opt/project",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "nyc-pipeline-tools:latest", "entrypoint-trino-bootstrap",
        ])


def run_dbt() -> None:
    if IS_K8S:
        _run(["kubectl", "delete", "job", "dbt-build", "-n", "nyc-taxi", "--ignore-not-found"])
        _run(["kubectl", "apply", "-f", "/repo/k8s/dbt/job.yaml", "-n", "nyc-taxi"])
        _run(["kubectl", "wait", "--for=condition=complete", "job/dbt-build", "-n", "nyc-taxi", "--timeout=180s"])
    else:
        _run([
            "docker", "run", "--rm",
            "--network", "nyc_new_default",
            "-v", f"{REPO_HOST}:/opt/project",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "nyc-dbt:latest", "entrypoint-dbt",
        ])


def run_superset_bootstrap() -> None:
    if IS_K8S:
        _run(["kubectl", "exec", "-n", "nyc-taxi", "deploy/superset", "--", "bash", "/app/docker/bootstrap_superset.sh"])
    else:
        _run([
            "docker", "exec", "nyc_superset",
            "bash", "/app/docker/bootstrap_superset.sh",
        ])


def validate_analytics() -> None:
    env = os.environ.copy()
    if IS_K8S:
        env["TRINO_HOST"] = "svc-trino"
        env["TRINO_PORT"] = "8080"
    else:
        env["TRINO_HOST"] = "trino-coordinator"
        env["TRINO_PORT"] = "8080"

    result = subprocess.run(
        [sys.executable, str(REPO_INSIDE_AIRFLOW / "scripts" / "run_analytics_questions.py")],
        cwd=REPO_INSIDE_AIRFLOW, capture_output=True, text=True, env=env,
    )
    log.info("stdout: %s", result.stdout)
    if result.returncode != 0:
        log.error("stderr: %s", result.stderr)
        raise RuntimeError(f"analytics failed:\n{result.stderr}")


def run_spark_batch(logical_date_str: str) -> None:
    # logical_date_str có định dạng "YYYY-MM-DD" truyền từ Jinja Template của Airflow (ví dụ: "2024-01-01")
    dt = datetime.strptime(logical_date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")  # Trả về định dạng hai chữ số: "01", "02", "03"...

    if IS_K8S:
        # Tự động chọn đúng file Job YAML theo tháng động trong Kubernetes
        _run(["kubectl", "delete", "job", f"spark-batch-m{month}", "-n", "nyc-taxi", "--ignore-not-found"])
        _run(["kubectl", "apply", "-f", f"/repo/k8s/jobs/spark-batch-m{month}.yaml", "-n", "nyc-taxi"])
        _run(["kubectl", "wait", "--for=condition=complete", f"job/spark-batch-m{month}", "-n", "nyc-taxi", "--timeout=600s"])
    else:
        # Chạy Local Docker
        _run([
            "docker", "run", "--rm",
            "--network", "nyc_new_default",
            "-v", f"{REPO_HOST}:/opt/project",
            "-e", "MINIO_ENDPOINT=http://minio:9000",
            "-e", "MINIO_ACCESS_KEY=minio",
            "-e", "MINIO_SECRET_KEY=minio123",
            "apache/spark:3.5.1",
            "/opt/spark/bin/spark-submit",
            "--master", "local[*]",
            "--conf", "spark.jars.ivy=/opt/project/.ivy2",
            "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "/opt/project/jobs/spark_local_batch.py",
            "--input", f"/opt/project/data/raw/yellow_taxi/yellow_tripdata_{year}-{month}.parquet",
            "--lookup", "/opt/project/data/lookup/taxi_zone_lookup.csv",
            "--silver", "/opt/project/data/silver/trips",
            "--quarantine", "/opt/project/data/quarantine/invalid_trips"
        ])


with DAG(
    dag_id="nyc_e2e_pipeline",
    description="NYC Taxi full pipeline: Spark -> Trino -> dbt -> Superset",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1), # Khởi chạy lịch sử từ 01/01/2024
    schedule="@monthly",             # Chu kỳ chạy hàng tháng
    catchup=True,                    # Bật Catchup để tự động chạy bù dữ liệu quá khứ từ start_date
    max_active_runs=1,               # Chạy tuần tự từng tháng một để tránh xung đột ghi đè S3
    tags=["nyc", "e2e"],
) as dag:
    
    # Task Spark Batch duy nhất nhận logical date động qua Jinja Template
    spark_batch = PythonOperator(
        task_id="spark_batch",
        python_callable=run_spark_batch,
        op_args=["{{ ds }}"]  # "{{ ds }}" trả về ngày chu kỳ dạng "YYYY-MM-DD"
    )

    spark_streaming = PythonOperator(task_id="spark_streaming", python_callable=run_spark_streaming)
    trino_bootstrap = PythonOperator(task_id="trino_bootstrap", python_callable=run_trino_bootstrap)
    dbt_build = PythonOperator(task_id="dbt_build", python_callable=run_dbt)
    superset_bootstrap = PythonOperator(task_id="superset_bootstrap", python_callable=run_superset_bootstrap)
    analytics_check = PythonOperator(task_id="analytics_check", python_callable=validate_analytics)

    # Cấu hình luồng phụ thuộc rõ ràng, tuyến tính
    spark_batch >> trino_bootstrap
    spark_streaming >> trino_bootstrap
    
    trino_bootstrap >> dbt_build >> superset_bootstrap >> analytics_check
