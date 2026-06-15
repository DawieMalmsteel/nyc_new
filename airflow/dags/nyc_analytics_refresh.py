"""DAG: nyc_analytics_refresh

Refresh analytics layer (assumes Spark streaming already running):
  1. dbt build (rebuild Trino views + data quality tests).
  2. gold_export (CTAS Parquet to MinIO).
  3. materialize_postgres (copy gold tables to Postgres analytics DB).
  4. Superset bootstrap (register DBs, datasets, charts).
  5. Analytics SQL validation.

Schedule: weekly; manual trigger for ad-hoc refresh.
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
    "execution_timeout": timedelta(minutes=30),
}

project_volume = k8s.V1Volume(
    name="project-files",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="project-files-pvc"
    ),
)
project_volume_mount = k8s.V1VolumeMount(
    name="project-files", mount_path="/opt/project"
)


with DAG(
    dag_id="nyc_analytics_refresh",
    description="Refresh: dbt + gold + Postgres + Superset",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "analytics"],
) as dag:

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
        service_account_name="airflow-sa",
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
        service_account_name="airflow-sa",
    )

    materialize_postgres = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="pg-materialize",
        task_id="materialize_postgres",
        cmds=["python3"],
        arguments=["/opt/project/scripts/materialize_to_postgres.py"],
        env_vars=[
            k8s.V1EnvVar(name="TRINO_HOST", value="svc-trino"),
            k8s.V1EnvVar(name="TRINO_PORT", value="8080"),
            k8s.V1EnvVar(name="PG_ANALYTICS_HOST", value="svc-postgres-analytics"),
            k8s.V1EnvVar(name="PG_ANALYTICS_USER", value="analytics"),
            k8s.V1EnvVar(name="PG_ANALYTICS_PASSWORD", value="analytics"),
            k8s.V1EnvVar(name="PG_ANALYTICS_DB", value="nyc_analytics"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
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
            k8s.V1EnvVar(
                name="SUPERSET_URL", value="http://svc-superset:8088"
            ),
            k8s.V1EnvVar(
                name="TRINO_URI",
                value="trino://analytics@svc-trino:8080/hive",
            ),
            k8s.V1EnvVar(
                name="PG_ANALYTICS_URI",
                value="postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics",
            ),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
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
        service_account_name="airflow-sa",
    )

    dbt_build >> gold_export >> materialize_postgres >> superset_bootstrap >> analytics_check
