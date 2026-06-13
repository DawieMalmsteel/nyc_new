# 6. Airflow DAGs — Pipeline Orchestration

## 6.1 Tổng quan

Apache Airflow 2.10.5 là công cụ điều phối chính trên Kubernetes. 
Pipeline có **3 DAGs**:

| DAG | Schedule | Mô tả | Tasks |
|-----|----------|-------|-------|
| `nyc_e2e_pipeline` | @monthly | Full E2E: Spark → Trino → dbt → Superset | 7 |
| `nyc_cdc_pipeline` | manual | CDC: Seed Postgres → Debezium → Bridge | 3 |
| `nyc_analytics_refresh` | @weekly | Refresh: dbt → Superset → Analytics | 4 |

### Cấu hình chung

Tất cả DAGs sử dụng **KubernetesPodOperator** (từ `apache-airflow-providers-cncf-kubernetes==8.4.2`):

```python
project_volume = k8s.V1Volume(
    name="project-files",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="project-files-pvc"
    )
)
project_volume_mount = k8s.V1VolumeMount(
    name="project-files",
    mount_path="/opt/project"
)
```

**Service Account**: `airflow-sa` (định nghĩa trong Helm chart RBAC)

---

## 6.2 DAG: nyc_e2e_pipeline

**File**: `airflow/dags/nyc_e2e_pipeline.py`

DAG chính — xử lý end-to-end cho mỗi tháng dữ liệu.

### Luồng thực thi

```
spark_batch ──┐
              ├──→ trino_bootstrap → dbt_build → gold_export → superset_bootstrap → analytics_check
spark_streaming ┘
```

### 7 Tasks chi tiết

#### Task 1: spark_batch
```python
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    cmds=["/opt/spark/bin/spark-submit"],
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,...",
        "--conf", "spark.jars.ivy=/opt/project/.ivy2",
        "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
        "/opt/project/jobs/spark_local_batch.py",
        "--input", "s3a://nyc-raw/yellow_taxi/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/yellow_tripdata_{{ logical_date.strftime('%Y') }}-{{ logical_date.strftime('%m') }}.parquet",
        "--lookup", "s3a://nyc-lookup/taxi_zone_lookup.csv",
    ],
    env_vars=[
        k8s.V1EnvVar(name="MINIO_ENDPOINT", value="http://svc-minio:9000"),
        k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value="minio"),
        k8s.V1EnvVar(name="MINIO_SECRET_KEY", value="minio123"),
    ],
    # ...
)
```

**Đặc điểm**:
- Dùng `logical_date` (Jinja template) để lấy year/month từ schedule
- Chạy `local[*]` — single pod, không cluster mode
- S3A packages via `--packages` CLI (không phải SparkSession config)
- Ivy cache tại `/opt/project/.ivy2/` (shared PVC)

#### Task 2: spark_streaming
```python
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,...",
        "/opt/project/jobs/spark_stream_taxi_events.py",
        "--bootstrap-server", "svc-kafka:9092",
        "--topic", "taxi.trip.events",
        "--trigger-available-now",
    ],
)
```

**Đặc điểm**:
- Kafka bootstrap: `svc-kafka:9092` (⚠️ có prefix `svc-`)
- `--trigger-available-now`: one-shot micro-batch (không phải streaming liên tục)
- Cần `spark-sql-kafka-0-10_2.12:3.5.1` package

#### Task 3: trino_bootstrap
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    cmds=["entrypoint-trino-bootstrap"],
    env_vars=[...],
)
```
Chạy `scripts/trino_register.py` — register Hive external tables.

#### Task 4: dbt_build
```python
KubernetesPodOperator(
    image="nyc-dbt:k8s",
    cmds=["entrypoint-dbt"],
    env_vars=[("DBT_PROFILES_DIR", "/opt/project/dbt"), ("TRINO_HOST", "svc-trino")],
)
```

#### Task 5: gold_export
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    cmds=["python3"],
    arguments=["/opt/project/scripts/export_gold_to_minio.py"],
)
```

#### Task 6: superset_bootstrap
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    arguments=["/opt/project/scripts/superset_bootstrap.py"],
    env_vars=[
        ("SUPERSET_URL", "http://svc-superset:8088"),
        ("TRINO_URI", "trino://analytics@svc-trino:8080/hive/mart"),
    ],
)
```

#### Task 7: analytics_check
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    arguments=["/opt/project/scripts/run_analytics_questions.py"],
    env_vars=[("TRINO_HOST", "svc-trino"), ("TRINO_PORT", "8080")],
)
```

### Schedule
```python
schedule="@monthly",
start_date=datetime(2024, 1, 1),
end_date=datetime(2024, 3, 31),
catchup=True,
```

Chạy backfill cho tháng 1-3/2024. Với `catchup=True`, Airflow tự động 
tạo DAG runs cho tất cả tháng từ start_date đến end_date, mỗi run 
xử lý 1 tháng với logical_date tương ứng.

---

## 6.3 DAG: nyc_cdc_pipeline

**File**: `airflow/dags/nyc_cdc_pipeline.py`

CDC pipeline — chỉ chạy manual (`schedule=None`).

### Luồng thực thi
```
cdc_seed → cdc_register → cdc_bridge
```

### 3 Tasks

#### Task 1: cdc_seed
```python
KubernetesPodOperator(
    cmds=["entrypoint-cdc-seed"],
    arguments=[
        "--input", "/opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet",
        "--max-rows", "5000",
        "--dsn", "postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi",
    ],
)
```
Đọc Parquet, insert 5000 rows vào Postgres.

#### Task 2: cdc_register
```python
KubernetesPodOperator(
    cmds=["entrypoint-cdc-register"],
    arguments=[
        "--debezium-url", "http://svc-debezium:8083",
        "--postgres-host", "svc-postgres-cdc",
    ],
)
```
Register Debezium Postgres connector qua REST API.

#### Task 3: cdc_bridge
```python
KubernetesPodOperator(
    cmds=["entrypoint-cdc-bridge"],
    arguments=[
        "--bootstrap-server", "svc-kafka:9092",
        "--input-topic", "nyc_cdc.public.trips",
        "--output-topic", "taxi.trip.events",
        "--idle-timeout", "30",
        "--flush-interval", "500",
    ],
)
```
Bridge CDC events → standard format, exits sau 30s idle.

---

## 6.4 DAG: nyc_analytics_refresh

**File**: `airflow/dags/nyc_analytics_refresh.py`

Refresh analytics layer — chạy @weekly.

### Luồng thực thi
```
dbt_build → gold_export → superset_bootstrap → analytics_check
```

### 4 Tasks

Giống với 4 tasks cuối của `nyc_e2e_pipeline`.

```python
schedule="@weekly",
start_date=datetime(2026, 1, 1),
catchup=False,
```

---

## 6.5 K8s Service Account & RBAC

**File**: `charts/nyc-taxi/templates/airflow/rbac.yaml`

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-sa
  namespace: nyc-taxi
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-role
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list", "watch", "create", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-rolebinding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: airflow-role
subjects:
- kind: ServiceAccount
  name: airflow-sa
```

---

## 6.6 Trigger DAGs

### Kubernetes (Primary) ⭐

```bash
# Qua Airflow Web UI (khuyến nghị)
# http://localhost:39085 → admin/admin → unpause DAG → Trigger

# Qua CLI (kubectl exec)
kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_e2e_pipeline

kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_cdc_pipeline

kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_analytics_refresh
```

### Docker Compose (Legacy)
```bash
make airflow-trigger DAG=nyc_e2e_pipeline
```

---

## 6.7 Airflow Entrypoint

**File**: `docker/entrypoint-airflow.sh`

```bash
case "${AIRFLOW_ROLE:-webserver}" in
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  init)
    airflow db migrate
    airflow users create --username admin --password admin --role Admin
    ;;
esac
```

Airflow dùng **LocalExecutor** với PostgreSQL làm metadata DB.

---

## 6.8 Xử lý lỗi DAG

### Pod execution timeout
```python
DEFAULT_ARGS = {
    "execution_timeout": timedelta(minutes=30),  # e2e pipeline
    # hoặc
    "execution_timeout": timedelta(minutes=15),  # analytics refresh
}
```

### Retry policy
```python
# nyc_cdc_pipeline có retry
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}
# e2e và analytics không retry (chạy lại từ đầu)
```

### Logging
```python
# get_logs=True trong KubernetesPodOperator
# Cho phép xem logs ngay trên Airflow UI
```
