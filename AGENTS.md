# Repository Guidelines

## Project Overview

NYC Taxi data pipeline — batch + streaming data engineering pipeline with two deployment modes: **Docker Compose** (local dev) and **Kubernetes (kind)** (production-like). Ingests NYC TLC trip records (Parquet), processes with Spark (enrichment + validation), stores silver/quarantine data in **MinIO S3**, exposes via Trino (Hive catalog), transforms with dbt-trino into analytics marts, and visualizes via Apache Superset. Also supports Debezium CDC from Postgres → Kafka as an alternative event source.

On Kubernetes, **Airflow** is the primary orchestrator — pipeline runs automatically on schedule. **Skaffold dev** is the primary deployment tool for K8s mode — single command deploys everything, auto-syncs file changes, and manages port-forwards. Makefile is for local Docker Compose dev/testing only.

---

## Architecture & Data Flow

```
  Batch:                      Streaming:
  Raw Parquet                 Kafka Producer (generator)
      ↓                              ↓
  Spark Batch (local[*])      Spark Streaming (Kafka)
      ↓                              ↓
      └──→ MinIO S3 (silver / quarantine) ←──┘
                    ↓
              Trino (Hive catalog + S3 connector)
                    ↓
              dbt-trino (15 models, 24/24 PASS)
                    ↓
              Apache Superset (7 datasets, dashboard)
                    ↓
              Airflow orchestration
```

**Validation rules** (in both Spark batch & streaming):
- `pickup_ts`, `dropoff_ts` must not be null
- `dropoff_ts` > `pickup_ts`
- `trip_distance` > 0, `fare_amount` >= 0, `total_amount` >= `fare_amount`
- `passenger_count` between 1–6
- `payment_type` between 1–6
- `pickup_location_id` / `dropoff_location_id` must exist in zone lookup

Valid → `s3a://nyc-silver/trips/` (partitioned by `pickup_year`, `pickup_month`).
Invalid → `s3a://nyc-quarantine/invalid_trips/`.

**Storage:**
- MinIO S3 (`s3a://` for Spark, `s3://` for Trino) for all pipeline data
- Local filesystem for Hive metastore and streaming checkpoints

---

## Key Directories

|Directory|Purpose|
|---|---|
|`jobs/`|Spark processors: `spark_local_batch.py` (batch), `spark_stream_taxi_events.py` (Kafka streaming)|
|`scripts/`|Utility scripts: CDC (seed/register/bridge), Trino bootstrap, Superset bootstrap, mart/analytics verification, `k8s_ui.sh` (port-forward)|
|`airflow/dags/`|DAGs: `nyc_e2e_pipeline` (full pipeline), `nyc_cdc_pipeline` (CDC), `nyc_analytics_refresh` (dbt → Superset → analytics)|
|`dbt/`|dbt-trino models (15 models: staging → marts → gold) + YAML + SQL tests|
|`docker/`|Dockerfiles, entrypoint scripts (`.sh`), Trino/Superset configs|
|`charts/`|**Helm chart** (`nyc-taxi`) — K8s manifests for all services, deployed via skaffold|
|`k8s/`|Legacy K8s manifests (kind cluster): raw YAML, still usable for reference|
|`sql/`|Analytics SQL questions (`analytics_questions.sql`), smoke tests|
|`data/`|Data lake: raw/silver/quarantine/lookup/checkpoints (gitignored)|
|`terraform/`|Terraform configs for MinIO bucket management (`aws_s3_bucket` resources)|
|`skaffold.yaml`|**Skaffold config** — Helm deployer, build artifacts, deploy hooks, sync rules, port-forwards|

---

## Development Commands

### Docker Compose (local dev)
All operations via `make <target>`.

#### Infrastructure
```
make infra-up            # Start core: ZK, Kafka, MinIO, Spark
make infra-up-all        # Everything (Trino, dbt, Superset, Airflow)
make infra-status        # docker compose ps
make infra-logs SVC=trino
```

#### Kafka
```
make kafka-topics        # Create topics (taxi.trip.events, .invalid, .dlq)
```

#### CDC (Debezium)
```
make cdc-seed            # Seed Postgres from parquet (5000 rows)
make cdc-register        # Register Debezium connector
make cdc-bridge          # Bridge CDC topic → taxi.trip.events
make cdc-verify          # Full CDC E2E
```

#### Spark
```
make spark-batch         # Batch backfill via MinIO S3
MONTH=03 make spark-batch  # Specific month
make spark-streaming     # Submit streaming job to Spark master
```

#### Trino
```
make trino-bootstrap     # Register tables from S3 parquet (idempotent)
make trino-shell         # Interactive Trino shell
```

#### dbt
```
make dbt-build           # Full dbt build (models + tests)
make dbt-run             # Models only
make dbt-test            # Tests only
```

#### Superset
```
make superset-bootstrap  # Register DB, 7 datasets, 4 charts, dashboard
make superset-check      # List resources
```

#### Airflow
```
make airflow-up          # Start Airflow (after infra-up)
make airflow-trigger DAG=nyc_analytics_refresh
```

#### Verify & Clean
```
make verify-all          # Full pipeline verification
make verify-mart         # Row counts in Trino
make verify-analytics    # 10 SQL questions (expect PASS 10/10)
make clean-all           # Delete generated data
```

### Kubernetes / Skaffold (primary)

**Skaffold** (`skaffold dev`) is the primary deployment tool. Single command builds images, syncs files to PVC, deploys Helm chart, and starts port-forwards. Watches for file changes and auto-syncs.

```bash
# Full development loop (auto-watch + sync + port-forward):
skaffold dev --namespace nyc-taxi

# One-shot deploy (no watch):
skaffold run --namespace nyc-taxi

# Build images only:
skaffold build --namespace nyc-taxi
```

After `skaffold dev` is running:
- Edit `airflow/dags/` → files auto-synced to PVC via file-sync pod → Airflow picks up
- Edit `jobs/` or `scripts/` → files auto-synced to PVC
- Edit Helm chart or Dockerfiles → skaffold auto-rebuilds + re-deploys

#### Legacy Makefile K8s targets (replaced by skaffold):
```
make k8s-cluster         # kind create cluster (3 nodes)
make k8s-images          # Build + load images into kind
make k8s-destroy         # kind delete cluster (all data gone)
make k8s-ui              # Start port-forwards (39080-39086) — alternative to skaffold port-forwards
make k8s-ui-stop         # Stop port-forwards
make k8s-verify          # Row counts via Trino job
make k8s-verify-analytics # 10 SQL questions job
make k8s-verify-cdc      # Postgres/Debezium/Kafka check
make k8s-clean           # Clean MinIO data + delete jobs
make k8s-status          # kubectl get pods
make k8s-logs JOB=name   # Tail logs for a job
```

---

## Code Conventions & Common Patterns

### Python
- **argparse** for CLI (no click/typer). All scripts use `parser.add_argument()` with typed defaults.
- **Type hints** on all function signatures, return types annotated.
- **Config/constants** at module top — named constants in `UPPER_CASE`, schema dicts as module-level variables.
- **Docstrings** on modules and functions (triple-quoted).
- **Error handling**: `try/except` around external calls (Kafka, REST API), `log.error` + `raise` on failure. Fail-fast in entrypoints via `set -euo pipefail` (bash).
- **Imports**: stdlib first, then third-party, then local. No `__init__.py` re-exports.
- **Main guard**: `if __name__ == "__main__": main()` pattern with `sys.exit(main())` for CLI return codes.
- **Entrypoint scripts** in `docker/` — minimal bash wrappers delegating to Python; `set -euo pipefail` and `exec`.

### Spark (PySpark)
- `SparkSession.builder.appName(...)` with `local[*]` master for batch, `spark://spark-master:7077` for streaming.
- **Schemas** defined as `StructType([StructField(...)])` lists, not DDL strings.
- Transformations use `spark.sql.functions` (not raw SQL in streaming).
- Column expressions via `col(...)`.
- Zone lookup join: small lookup DF directly joined without explicit broadcast hint.
- Stream processing uses `foreachBatch` + `writeStream.trigger(availableNow=True)` for batch-mode consumption.
- Output partitioned by `pickup_year`, `pickup_month`.
- **MinIO S3 config**: `spark.hadoop.fs.s3a.*` with env var overrides (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`). Credentials hardcoded `minio/minio123`.
- **S3A packages**: Must use `--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262` on `spark-submit` CLI. Using `spark.jars.packages` in SparkSession config fails at runtime.
- Both batch and streaming always use `mode("append")` — never `overwrite` (to avoid data loss from `partitionOverwriteMode=dynamic`).
- **S3 commit fix**: `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` required because MinIO does not support atomic rename.
- **Ivy cache**: `spark.jars.ivy=/opt/project/.ivy2` shared on PVC to avoid re-downloading hadoop-aws deps per pod.

### dbt (SQL)
- **Naming**: `stg_` (staging), `dim_`/`fact_` (marts), `gold_` (gold layer), `mart_` (summary).
- **Materialization**: All models are `view`. Hive file-based HMS does not support `RENAME TABLE` (which dbt uses for table swaps). Never use `materialized='table'`.
- **Model layers**: 4 staging (stg_trips, stg_zones, stg_invalid_trips), 7 marts (fact_trips, dim_zone, fact_invalid_trips, mart_hourly_summary, mart_revenue_by_day, mart_revenue_by_zone, mart_payment_type_summary), 4 gold.
- **Tests**: YAML generic tests (`not_null`, `accepted_values`) per model; singular SQL tests in `dbt/tests/` (e.g., `payment_type_range.sql`).
- **Refs**: Models reference each other via `{{ ref('model_name') }}`. No direct table references across layers.
- **Derived fields**: `tip_rate = tip_amount / total_amount`, `trip_duration_sec` via `date_diff`.

### Docker Compose
- **Profiles** for service grouping: `default` (core), `tools`, `trino`, `dbt`, `superset`, `airflow`.
- One-shot services (`restart: "no"`) vs daemon services (`restart: unless-stopped`).
- Tools image (`nyc-pipeline-tools:latest`) — Python 3.11, includes `kafka-python`, `psycopg2-binary`, `pyarrow`, `pandas`, `sqlalchemy-trino`, `trino`.
- MinIO credentials hardcoded `minio/minio123` across Spark config, Trino catalog, and mc client.

### Kubernetes (kind) + Skaffold (Helm)
- **3 nodes**: 1 control-plane + 2 workers. Node affinity on `kind-worker` for PVC access (RWO).
- **hostPath PVCs**: `raw-data-pv` → `/mnt/nyc-data`, `project-files-pv` → `/mnt/nyc-project`.
- Custom images built via Skaffold: `nyc-pipeline-tools:k8s`, `nyc-dbt:k8s`, `nyc-airflow:k8s`.
- **Skaffold Helm deployer**: `skaffold.yaml` defines 3 artifacts, sync rules, deploy hooks, and port-forwards.
- **Deploy hooks** (pre-deploy): delete immutable Jobs, sync project files to kind-worker PVC.
- **Sync rules**: watch local `airflow/dags/`, `jobs/`, `scripts/`, `dbt/`, `charts/` — auto-push to PVC via `file-sync` pod.
- **File-sync pod**: `charts/.../airflow/file-sync.yaml` — lightweight `sleep infinity` container running as root, PVC mounted at `/opt/project`, target for `skaffold sync`.
- **Port-forwards** via skaffold `portForward` (39080-39087), or legacy `make k8s-ui` (uses `kubectl port-forward --address 0.0.0.0`).
- Services use `ClusterIP` type. No Ingress.
- **Lưu ý**: Sau lần helm install đầu tiên, xóa namespace `nyc-taxi` cũ nếu nó bị stuck ở `Terminating`: `kubectl delete namespace nyc-taxi --force --grace-period=0`.

### Airflow (DAGs)
- **KubernetesPodOperator** (not BashOperator) for K8s mode. Pods mount `project-files-pvc` at `/opt/project`.
- **claim_name** (snake_case) for kubernetes client v29.0.0 volume config.
- `IS_K8S` flag auto-detects environment via `KUBERNETES_SERVICE_HOST` env var.
- Schedules: `nyc_e2e_pipeline` @monthly, `nyc_analytics_refresh` @weekly, `nyc_cdc_pipeline` @monthly.
- Spark streaming task: uses `--bootstrap-server svc-kafka:9092` (⚠️ **không phải** `kafka:9092` — service name trong K8s là `svc-kafka`).
### CDC (Debezium)
- Postgres 16 with WAL logical replication (`wal_level=logical`).
- Debezium Kafka Connect 2.5 — Postgres connector, `ExtractNewRecordState` SMT.
- Bridge script (`scripts/cdc_bridge.py`) reduces JSON envelope to flat format compatible with Spark schema.
- **Poll-based loop**: Uses `consumer.poll()` with `--idle-timeout` to exit after N idle seconds (not infinite `for msg in consumer:` iterator).
- **Async optimization**: Default mode uses `producer.send()` + periodic flush every `--flush-interval` events. Sync mode (`--sync`) forces `producer.send().get()` per event — ~50x slower.
- Performance: ~300-500 ev/s async vs ~9 ev/s sync.

---

## Important Files

|File|Purpose|
|---|---|
|`jobs/spark_local_batch.py`|Batch backfill — enrichment + validation, writes S3|
|`jobs/spark_stream_taxi_events.py`|Kafka streaming consumer — same logic as batch|
|`dbt/models/marts/fact_trips.sql`|Primary fact table with derived fields|
|`dbt/models/staging/stg_trips.sql`|Clean column types from silver Parquet|
|`dbt/models/gold/gold_fact_trips.sql`|Gold-level fact with trip_id, source_file|
|`scripts/trino_register.py`|Register Hive tables pointing to S3 paths|
|`scripts/cdc_bridge.py`|CDC topic → standard event format (poll-based async)|
|`scripts/superset_bootstrap.py`|Idempotent Superset setup (7 datasets, charts, dashboard)|
|`scripts/run_analytics_questions.py`|10 SQL analytics queries validated against Trino|
|`scripts/verify_mart.py`|Row count verification of 4 mart tables|
|`scripts/k8s_ui.sh`|Port-forward manager using `setsid -f` for survival after `make` exit|
|`docker/tools.Dockerfile`|Base image (Python 3.11, kafka-python, trino, pandas, pyarrow). **Copies all `docker/*.sh`** + creates symlinks for all (`entrypoint-*` + `wait-kafka`)|
|`docker/dbt.Dockerfile`|dbt-trino runner image|
|`docker/airflow.Dockerfile`|Airflow 2.10.5 image with Docker Compose + providers|
|`docker/entrypoint-init-postgres.sh`|Postgres init — uses **Python psycopg2** (không `psql`, không cài postgresql-client)|
|`docker/entrypoint-topic-init.sh`|Kafka topic init — uses `wait-kafka` + **`svc-kafka:9092`**|
|`docker/wait-kafka.sh`|TCP wait script for Kafka bootstrap readiness (up to 120s)|
|`docker/entrypoint-cdc-*.sh`|CDC bridge/seed/register entrypoints|
|`docker-compose.yml`|16+ services, 6 profiles, 3 named volumes|
|`Makefile`|Single entry point (40+ targets, 9 groups) — Docker Compose mode only|
|`skaffold.yaml`|**Skaffold v4beta3 config** — Helm deployer, 3 artifacts, deploy hooks, sync rules, port-forwards|
|`charts/nyc-taxi/`|**Helm chart** — all K8s service manifests (airflow, spark, kafka, trino, minio, superset, debezium, etc.)|
|`charts/nyc-taxi/templates/airflow/file-sync.yaml`|**File-sync pod** — `sleep infinity`, runs root, PVC mounted, target for hot-reload sync|
|`charts/nyc-taxi/templates/namespace/namespace.yaml`|Namespace template with Helm labels/annotations|
|`charts/nyc-taxi/templates/jobs/topic-init.yaml`|Kafka topic init job — uses `entrypoint-topic-init` (wait-kafka + `svc-kafka:9092`)|
|`charts/nyc-taxi/templates/jobs/postgres-init.yaml`|Postgres init job — uses `entrypoint-init-postgres` (Python psycopg2)|
|`kind.yaml`|kind cluster config (3 nodes, port mappings)|
|`k8s/`|Legacy K8s manifests (raw YAML, replaced by Helm chart)|
|`airflow/dags/nyc_e2e_pipeline.py`|E2E pipeline DAG (spark → trino → dbt → superset). ⚠️ Dùng **`svc-kafka:9092`** cho spark_streaming|
|`airflow/dags/nyc_cdc_pipeline.py`|CDC pipeline DAG: seed Postgres → register Debezium → bridge CDC events to Kafka|
|`airflow/dags/nyc_analytics_refresh.py`|Analytics refresh DAG: dbt → Superset refresh → analytics check|
|`check.md`|Quick reference: UI URLs, credentials, port-forwards, row counts|
|`sql/analytics_questions.sql`|10 business SQL queries against mart tables|

---

## Runtime/Tooling Preferences

- **Deployment modes**: Kubernetes/kind (primary), Docker Compose (local dev).
- **Docker** is the only runtime requirement for Docker Compose mode. Host needs only Docker + Docker Compose.
- **kind** for local K8s. 3 nodes, hostPath PVCs, NodePort port mappings `38080-38088`.
- **Make** as single entry point (no shell aliases, no manual docker/kubectl commands).
- **Python 3.11** inside containers (tools image), **Spark 3.5.1** (`apache/spark:3.5.1`), **Trino 435**, **dbt-trino 1.11.x**, **Superset 4.0.0**, **Debezium 2.5**, **Airflow 2.10.5**.
- **MinIO** as S3-compatible storage: Spark uses `s3a://`, Trino uses `s3://`.
- **No linter/formatter** configured. Code style is conventional Python.
- **K8s port-forwards**: `--address 0.0.0.0` flag required. Use port range `39080+` (avoid kind NodePort conflict). Use `setsid -f` for survival.

### PVC Sync (tự động qua Skaffold)
Trong K8s mode, scripts và configs chạy từ PVC (`/opt/project/` mounted từ kind-worker), không phải từ container image.

**Tự động** (không cần thao tác thủ công):
- `skaffold dev` chạy pre-deploy hook sync toàn bộ file → kind-worker PVC
- Khi file thay đổi, `skaffold sync` push thẳng vào `file-sync` pod → PVC → Airflow nhận thay đổi

**Thủ công** (khi cần sync nhanh hoặc không dùng skaffold):
```bash
cd /home/dwcks/vsf_gsm/nyc_new
tar cf - \
  --exclude='dbt/logs' --exclude='dbt/target' \
  --exclude='.git' --exclude='__pycache__' \
  --exclude='*.pyc' --exclude='*.pyo' \
  airflow/dags/ jobs/ scripts/ dbt/ charts/ \
  | docker exec -i kind-worker tar xf - -C /mnt/nyc-project
```

---

## Testing & QA

### dbt Tests (`make dbt-build`)
- 15 models, 9 data tests — **24/24 PASS** expected.
- **Generic tests**: `not_null`, `accepted_values` in YAML test files per model.
- **Singular tests**: Custom SQL in `dbt/tests/` (e.g., `payment_type_range.sql`).
- Coverage: NOT NULL on key columns, payment_type accepted values (1–6), payment_type range sanity.

### Analytics Validation (`make verify-analytics`)
- 10 SQL questions run against Trino via `scripts/run_analytics_questions.py`.
- Each query must return ≥1 row. Expect **10/10 PASS**.

### Mart Verification (`make verify-mart`)
- Row counts via Trino `hive.mart.*` views.
- Expected: `dim_zone` = 261, `fact_trips` = ~8-10M, `mart_hourly` = ~11K+, `mart_revenue_by_day` = ~96.

### Full Pipeline (Airflow DAG `nyc_e2e_pipeline`)
8 tasks: Spark streaming + 3x Spark batch → Trino bootstrap → dbt build → Superset bootstrap → analytics check.
Trigger via Airflow UI (http://localhost:39085) or CLI (`make airflow-trigger DAG=nyc_e2e_pipeline`).
### Key Constraints
- **Hive HMS**: No `RENAME TABLE`. All dbt models **must** be `materialized='view'`. `materialized='table'` fails at build time.
- **MinIO credentials**: Hardcoded `minio/minio123` in Spark config, Trino catalog, and mc. Change everywhere if rotated.
- **Spark UID mismatch** (Docker): Spark runs as UID 185, host as 1000. `make setup-volumes` fixes data dir permissions (777) — only relevant for local FS mode, not S3.
- **Docker network**: Compose project `nyc_new` creates network `nyc_new_default`. Spark containers need this network to reach MinIO.
- **S3A packages**: Must pass via `--packages` on `spark-submit`, not in SparkSession config.
- **verify_mart.py** uses `SET SESSION query_max_run_time='120s'` to avoid timeout on large aggregate queries.
- **Service names in K8s**: Tất cả service names đều có prefix `svc-` (e.g., `svc-kafka`, `svc-minio`, `svc-postgres-cdc`). **Không** dùng tên thiếu prefix (e.g., `kafka` sẽ không resolve được DNS).
- **.ivy2 cache**: Nằm tại `/opt/project/.ivy2/` trên PVC. Cần permissions 777 nếu spark chạy với UID khác. Xóa cache nếu cần refresh dependencies.
- **Namespace stuck Terminating**: Nếu xóa namespace `nyc-taxi` và nó bị stuck: `kubectl replace --raw /api/v1/namespaces/nyc-taxi/finalize -f <(kubectl get namespace nyc-taxi -o json | python3 -c "import json,sys; ns=json.load(sys.stdin); ns['spec']['finalizers']=[]; print(json.dumps(ns))")`
