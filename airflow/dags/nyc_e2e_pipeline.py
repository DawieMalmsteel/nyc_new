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


with DAG(
    dag_id="nyc_e2e_pipeline",
    description="NYC Taxi full pipeline: Spark -> Trino -> dbt -> Superset",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 3, 31),
    schedule="@monthly",
    catchup=True,
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
        # Sử dụng Jinja Template lấy năm và tháng của chu kỳ chạy động
        arguments=[
            "--master", "local[*]",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "--conf", "spark.jars.ivy=/opt/project/.ivy2",
            "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
            "--conf", "spark.scheduler.mode=FAIR",
            "/opt/project/jobs/spark_local_batch.py",
            "--input", "s3a://nyc-raw/yellow_taxi/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/yellow_tripdata_{{ logical_date.strftime('%Y') }}-{{ logical_date.strftime('%m') }}.parquet",
            "--lookup", "s3a://nyc-lookup/taxi_zone_lookup.csv",
            "--silver", "s3a://nyc-silver/trips",
            "--quarantine", "s3a://nyc-quarantine/invalid_trips"
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
        service_account_name="airflow-sa"
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

    # 5. Superset Bootstrap
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

    # 6. Analytics Check
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

    # Khai báo luồng phụ thuộc tuyến tính tuyệt đẹp
    spark_batch >> trino_bootstrap
    spark_streaming >> trino_bootstrap
    
    trino_bootstrap >> dbt_build >> superset_bootstrap >> analytics_check
