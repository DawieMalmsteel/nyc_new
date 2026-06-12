"""DAG: nyc_cdc_pipeline

CDC pipeline: Seed Postgres → Register Debezium → Bridge CDC events to Kafka.
Uses KubernetesPodOperator to run each step in K8s pods.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nyc",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}

# Định nghĩa cấu hình Volume dùng chung
project_volume = k8s.V1Volume(
    name="project-files",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="project-files-pvc")
)
project_volume_mount = k8s.V1VolumeMount(
    name="project-files",
    mount_path="/opt/project"
)


with DAG(
    dag_id="nyc_cdc_pipeline",
    description="CDC: Seed Postgres → Debezium → Bridge to Kafka events",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Chỉ chạy manual hoặc từ DAG khác
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "cdc", "kafka"],
) as dag:

    # ─── 1. Seed Postgres ───────────────────────────────────────
    cdc_seed = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="cdc-seed",
        task_id="cdc_seed",
        cmds=["entrypoint-cdc-seed"],
        arguments=[
            "--input",
            "/opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet",
            "--max-rows",
            "5000",
            "--dsn",
            "postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi",
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
    )

    # ─── 2. Register Debezium Connector ──────────────────────────
    cdc_register = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="cdc-register",
        task_id="cdc_register",
        cmds=["entrypoint-cdc-register"],
        arguments=[
            "--debezium-url",
            "http://svc-debezium:8083",
            "--postgres-host",
            "svc-postgres-cdc",
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
    )

    # ─── 3. Bridge CDC → Kafka events ───────────────────────────
    cdc_bridge = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="cdc-bridge",
        task_id="cdc_bridge",
        cmds=["entrypoint-cdc-bridge"],
        arguments=[
            "--bootstrap-server",
            "svc-kafka:9092",
            "--input-topic",
            "nyc_cdc.public.trips",
            "--output-topic",
            "taxi.trip.events",
            "--idle-timeout",
            "30",
            "--flush-interval",
            "500",
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
    )

    # ─── Luồng thực thi ──────────────────────────────────────────
    cdc_seed >> cdc_register >> cdc_bridge
