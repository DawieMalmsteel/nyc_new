"""DAG: nyc_e2e_pipeline

End-to-end orchestration of the NYC Taxi pipeline.
Production-Grade design utilizing KubernetesPodOperator (EKS & Kind Ready).
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

# Định nghĩa cấu hình Volume Mount dùng chung cho các K8s Pods
project_volume = k8s.V1Volume(
    name="project-files",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="project-files-pvc")
)
project_volume_mount = k8s.V1VolumeMount(
    name="project-files",
    mount_path="/opt/project"
)

raw_data_volume = k8s.V1Volume(
    name="raw-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="raw-data-pvc")
)
raw_data_volume_mount = k8s.V1VolumeMount(
    name="raw-data",
    mount_path="/mnt/nyc-data"
)


with DAG(
    dag_id="nyc_e2e_pipeline",
    description="NYC Taxi full pipeline: Spark -> Trino -> dbt -> Superset (single-run demo)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    max_active_runs=1,
    tags=["nyc", "e2e"],
) as dag:

    # 1. Spark Batch (K8s Native Operator)
    spark_batch = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="apache/spark:3.5.1",
        image_pull_policy="IfNotPresent",
        name="spark-batch",
        task_id="spark_batch",
        cmds=["/opt/spark/bin/spark-submit"],
        # Đọc tất cả tháng cùng lúc bằng glob pattern
        arguments=[
            "--master", "local[*]",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "--conf", "spark.jars.ivy=/opt/project/.ivy2",
            "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
            "--conf", "spark.scheduler.mode=FAIR",
            "/opt/project/jobs/spark_local_batch.py",
            "--input", "s3a://nyc-raw/yellow_taxi/year=*/month=*/*.parquet",
            "--lookup", "s3a://nyc-lookup/taxi_zone_lookup.csv",
            "--silver", "s3a://nyc-silver/trips",
            "--quarantine", "s3a://nyc-quarantine/invalid_trips",
        ],
        env_vars=[
            k8s.V1EnvVar(name="MINIO_ENDPOINT", value="http://svc-minio:9000"),
            k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value="minio"),
            k8s.V1EnvVar(name="MINIO_SECRET_KEY", value="minio123"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        security_context=k8s.V1PodSecurityContext(run_as_user=0),
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa"
    )

    # 1b. CDC: Seed Postgres (demo: 1000 rows) → Register Debezium → Bridge to Kafka
    cdc_seed = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="cdc-seed",
        task_id="cdc_seed",
        cmds=["entrypoint-cdc-seed"],
        arguments=[
            "--input", "/mnt/nyc-data/data/nyc-raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet",
            "--max-rows", "1000",
            "--dsn", "postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi",
        ],
        volumes=[project_volume, raw_data_volume],
        volume_mounts=[project_volume_mount, raw_data_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
        is_delete_operator_pod=False,  # keep for debugging
    )

    cdc_register = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="cdc-register",
        task_id="cdc_register",
        cmds=["entrypoint-cdc-register"],
        arguments=[
            "--debezium-url", "http://svc-debezium:8083",
            "--postgres-host", "svc-postgres-cdc",
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
    )

    cdc_bridge = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="cdc-bridge",
        task_id="cdc_bridge",
        cmds=["entrypoint-cdc-bridge"],
        arguments=[
            "--bootstrap-server", "svc-kafka:9092",
            "--input-topic", "nyc_cdc.public.trips",
            "--output-topic", "taxi.trip.events",
            "--idle-timeout", "30",
            "--flush-interval", "500",
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
    )

    # 2. Spark Streaming (K8s Native Operator)
    spark_streaming = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="apache/spark:3.5.1",
        image_pull_policy="IfNotPresent",
        name="spark-streaming",
        task_id="spark_streaming",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            "--conf", "spark.jars.ivy=/opt/project/.ivy2",
            "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
            "--conf", "spark.scheduler.mode=FAIR",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "/opt/project/jobs/spark_stream_taxi_events.py",
            "--bootstrap-server", "svc-kafka:9092",
            "--topic", "taxi.trip.events",
            "--lookup-path", "s3a://nyc-lookup/taxi_zone_lookup.csv",
            "--silver-path", "s3a://nyc-silver/trips",
            "--quarantine-path", "s3a://nyc-quarantine/invalid_trips",
            "--checkpoint-path", "s3a://nyc-silver/checkpoints/spark_stream_taxi_events/taxi.trip.events",
            "--trigger-available-now"
        ],
        env_vars=[
            k8s.V1EnvVar(name="MINIO_ENDPOINT", value="http://svc-minio:9000"),
            k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value="minio"),
            k8s.V1EnvVar(name="MINIO_SECRET_KEY", value="minio123"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        security_context=k8s.V1PodSecurityContext(run_as_user=0),
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa"
    )

    # 3. Trino Bootstrap (K8s Native Operator)
    trino_bootstrap = KubernetesPodOperator(
        namespace="nyc-taxi",
        image="nyc-pipeline-tools:k8s",
        image_pull_policy="IfNotPresent",
        name="trino-bootstrap",
        task_id="trino_bootstrap",
        trigger_rule="one_success",  # run if either batch or streaming succeeds
        cmds=["entrypoint-trino-bootstrap"],
        env_vars=[
            k8s.V1EnvVar(name="TRINO_HOST", value="svc-trino"),
            k8s.V1EnvVar(name="TRINO_PORT", value="8080"),
            k8s.V1EnvVar(name="TRINO_USE_SSL", value="false"),
            k8s.V1EnvVar(name="S3_MODE", value="true"),
            k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID", value="minio"),
            k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="minio123"),
            k8s.V1EnvVar(name="AWS_ENDPOINT_URL", value="http://svc-minio:9000"),
            k8s.V1EnvVar(name="SILVER_PATH", value="s3://nyc-silver/trips"),
            k8s.V1EnvVar(name="QUARANTINE_PATH", value="s3://nyc-quarantine/invalid_trips"),
            k8s.V1EnvVar(name="ZONES_PATH", value="s3://nyc-lookup/"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa",
        startup_timeout_seconds=600,
        is_delete_operator_pod=False,
    )

    # 4. dbt Build (K8s Native Operator)
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

    # 5. Gold Export
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

    # 5b. Materialize gold tables into Postgres analytics DB
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

    # 6. Superset Bootstrap
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
            k8s.V1EnvVar(name="PG_ANALYTICS_URI", value="postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics"),
        ],
        volumes=[project_volume],
        volume_mounts=[project_volume_mount],
        get_logs=True,
        in_cluster=True,
        service_account_name="airflow-sa"
    )

    # 7. Analytics Check
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

    # Dependencies: CDC pipeline → streams → batch+stream converge at Trino
    cdc_seed >> cdc_register >> cdc_bridge >> spark_streaming
    spark_batch >> trino_bootstrap
    spark_streaming >> trino_bootstrap
    
    trino_bootstrap >> dbt_build >> gold_export
    dbt_build >> materialize_postgres >> superset_bootstrap >> analytics_check
