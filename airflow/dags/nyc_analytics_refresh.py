"""DAG: nyc_analytics_refresh

Refresh analytics layer (assumes Spark streaming already running):
  1. dbt build (rebuild views + data quality tests).
  2. Superset bootstrap (refresh dashboard assets).
  3. Analytics SQL validation.

Schedule: manual trigger; set schedule="@hourly" in production.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nyc",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=15),
}

# Định nghĩa cấu hình Volume Mount dùng chung cho K8s Pods
project_volume = k8s.V1Volume(
    name="project-files",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="project-files-pvc")
)
project_volume_mount = k8s.V1VolumeMount(
    name="project-files",
    mount_path="/opt/project"
)


with DAG(
    dag_id="nyc_analytics_refresh",
    description="Refresh: dbt + Superset + analytics validation",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "analytics"],
) as dag:

    # 1. dbt Build chạy trực tiếp dạng K8s Operator chuẩn chỉnh
    dbt_build = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-dbt:k8s",
        image_pull_policy="IfNotPresent",
        name="dbt-build",
        task_id="dbt_build",
        cmds=["entrypoint-dbt"],
        env_vars=[
            k8s.V1EnvVar(name="DBT_PROFILES_DIR", value="/opt/project/dbt"),
            k8s.V1EnvVar(name="TRINO_HOST", value="svc-trino"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa"
    )

    gold_export = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="gold-export",
        task_id="gold_export",
        cmds=["python3"],
        arguments=["/opt/project/scripts/export_gold_to_minio.py"],
        env_vars=[
            k8s.V1EnvVar(name="TRINO_HOST", value="svc-trino"),
            k8s.V1EnvVar(name="TRINO_PORT", value="8080"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa"
    )

    superset_bootstrap = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="superset-bootstrap",
        task_id="superset_bootstrap",
        cmds=["python3"],
        arguments=["/opt/project/scripts/superset_bootstrap.py"],
        env_vars=[
            k8s.V1EnvVar(name="SUPERSET_URL", value="http://svc-superset:8088"),
            k8s.V1EnvVar(name="TRINO_URI", value="trino://analytics@svc-trino:8080/hive/mart"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa"
    )

    analytics_check = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="analytics-check",
        task_id="analytics_check",
        cmds=["python3"],
        arguments=["/opt/project/scripts/run_analytics_questions.py"],
        env_vars=[
            k8s.V1EnvVar(name="TRINO_HOST", value="svc-trino"),
            k8s.V1EnvVar(name="TRINO_PORT", value="8080"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa"
    )

    # Luồng tuần tự
    dbt_build >> gold_export >> superset_bootstrap >> analytics_check
