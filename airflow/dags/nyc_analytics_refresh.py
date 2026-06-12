"""DAG: nyc_analytics_refresh

Refresh analytics layer (assumes Spark streaming already running):
  1. dbt build (rebuild views + data quality tests).
  2. Superset bootstrap (refresh dashboard assets).
  3. Analytics SQL validation.

Schedule: manual trigger; set schedule="@hourly" in production.
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
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "nyc",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=15),
}

# Absolute host path (bind mount into airflow container as /repo).
REPO_HOST = "/home/dwcks/vsf_gsm/nyc_new"
REPO_INSIDE_AIRFLOW = Path("/repo")

IS_K8S = os.path.exists("/var/run/secrets/kubernetes.io") or "KUBERNETES_SERVICE_HOST" in os.environ

# Định nghĩa cấu hình Volume Mount dùng chung cho K8s Pods
project_volume = k8s.V1Volume(
    name="project-files",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claimName="project-files-pvc")
)
project_volume_mount = k8s.V1VolumeMount(
    name="project-files",
    mount_path="/opt/project"
)


def _run(cmd: list[str]) -> None:
    log.info("exec: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        log.info("stdout: %s", result.stdout)
    if result.stderr:
        log.error("stderr: %s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"command failed (rc={result.returncode}): {' '.join(cmd)}")


def run_superset_bootstrap() -> None:
    """Khởi tạo và đồng bộ hóa Dashboard tài nguyên cho Superset."""
    if IS_K8S:
        # Sử dụng native K8s client API để thực thi lệnh trực tiếp bên trong container (EKS Ready)
        from kubernetes import client, config
        from kubernetes.stream import stream
        
        config.load_incluster_config()
        api = client.CoreV1Api()
        
        # Tìm đúng Pod Superset đang chạy trong namespace 'nyc-taxi'
        pods = api.list_namespaced_pod(namespace="nyc-taxi", label_selector="app=superset")
        if not pods.items:
            raise RuntimeError("Superset pod not found in namespace 'nyc-taxi'")
        pod_name = pods.items[0].metadata.name
        
        # Thực thi file script cấu hình của Superset
        exec_command = ["bash", "/app/docker/bootstrap_superset.sh"]
        resp = stream(
            api.connect_get_namespaced_pod_exec,
            pod_name,
            "nyc-taxi",
            command=exec_command,
            stderr=True, stdin=False,
            stdout=True, tty=False
        )
        log.info("Superset Bootstrap Response: %s", resp)
    else:
        _run([
            "docker", "exec", "nyc_superset",
            "bash", "/app/docker/bootstrap_superset.sh",
        ])


def validate_analytics() -> None:
    """Chạy kiểm định SQL cuối cùng."""
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

    superset_bootstrap = PythonOperator(task_id="superset_bootstrap", python_callable=run_superset_bootstrap)
    analytics_check = PythonOperator(task_id="analytics_check", python_callable=validate_analytics)

    # Luồng tuần tự
    dbt_build >> superset_bootstrap >> analytics_check
