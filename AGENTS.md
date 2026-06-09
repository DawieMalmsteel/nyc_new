# Repository Guidelines

## Project Overview

NYC Taxi data pipeline — batch + streaming data engineering pipeline with two deployment modes: **Docker Compose** (local dev) and **Kubernetes (kind)** (production-like). Ingests NYC TLC trip records (Parquet), processes with Spark (enrichment + validation), stores silver/quarantine data in **MinIO S3**, exposes via Trino (Hive catalog), transforms with dbt-trino into analytics marts, and visualizes via Apache Superset. Also supports Debezium CDC from Postgres → Kafka as an alternative event source.

All operations driven through **Makefile**; no manual Docker/kubectl command memorization needed.

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

| Directory | Purpose |
|---|---|
| `jobs/` | Spark processors: `spark_local_batch.py` (batch), `spark_stream_taxi_events.py` (Kafka streaming) |
| `scripts/` | Utility scripts: CDC (seed/register/bridge), Trino bootstrap, Superset bootstrap, mart/analytics verification |
| `airflow/dags/` | DAGs: `nyc_e2e_pipeline` (full pipeline), `nyc_analytics_refresh` (dbt → Superset → analytics) |
| `dbt/` | dbt-trino models (15 models: staging → marts → gold) + YAML + SQL tests |
| `docker/` | Dockerfiles, entrypoint scripts, Trino/Superset configs |
| `k8s/` | Kubernetes manifests (kind cluster): all services, storage, jobs |
| `sql/` | Analytics SQL questions (`analytics_questions.sql`), smoke tests |
| `generator/` | Kafka event generator (Python) |
| `data/` | Data lake: raw/silver/quarantine/lookup/checkpoints (gitignored) |

---

## Development Commands

All operations via `make <target>`. Makefile has 9 groups:

### Infrastructure
```
make infra-up            # Start core: ZK, Kafka, Kafka-UI, MinIO, Spark
make infra-up-all        # Everything (Trino, dbt, Superset, Airflow)
make infra-status        # docker compose ps
make infra-logs SVC=trino
```

### Kafka
```
make kafka-topics        # Create topics (taxi.trip.events, .invalid, .dlq)
make kafka-publish       # Publish 5000 events via generator
```

### CDC (Debezium)
```
make cdc-seed            # Seed Postgres from parquet (5000 rows)
make cdc-register        # Register Debezium connector
make cdc-bridge          # Bridge CDC topic → taxi.trip.events
make cdc-bridge-bench    # Benchmark CDC bridge (50K events)
make cdc-verify          # Full CDC E2E
```

### Spark
```
make spark-batch         # Batch backfill via MinIO S3
MONTH=03 make spark-batch  # Specific month
make spark-streaming     # Submit streaming job to Spark master
```

### Trino
```
make trino-bootstrap     # Register tables from S3 parquet (idempotent)
make trino-shell         # Interactive Trino shell
make trino-sync-parts   # Sync Hive partitions
```

### dbt
```
make dbt-build           # Full dbt build (models + tests)
make dbt-run             # Models only
make dbt-test            # Tests only
```

### Superset
```
make superset-bootstrap  # Register DB, 7 datasets, 4 charts, dashboard
make superset-check      # List resources
# UI: http://localhost:8088 (admin/admin)
```

### Airflow
```
make airflow-up          # Start Airflow (after infra-up)
make airflow-trigger DAG=nyc_analytics_refresh
# UI: http://localhost:8085 (admin/admin)
```

### K8s (kind)
```
make k8s-cluster         # kind create cluster
make k8s-images          # Build + load images into kind
make k8s-deploy          # Deploy all manifests
make k8s-pipeline        # Run pipeline (jobs in order)
make k8s-verify          # Verify via Trino
make k8s-status          # kubectl get pods
make k8s-down            # kind delete cluster
```

### Verify & Clean
```
make verify-all          # Full pipeline: batch → Trino → dbt → analytics → Superset
make verify-mart         # Row counts in Trino
make verify-analytics    # 10 SQL questions (expect PASS 10/10)
make clean-all           # Delete generated data
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
- Tools image (`nyc-pipeline-tools:latest`) — Python 3.11, includes `kafka-python`, `psycopg2-binary`, `pyarrow`, `pandas`, `sqlalchemy-trino`.
- MinIO credentials hardcoded `minio/minio123` across Spark config, Trino catalog, and mc client.

### Kubernetes (kind)
- **3 nodes**: 1 control-plane + 2 workers. Node affinity on `kind-worker` for PVC access (RWO).
- **hostPath PVCs**: `raw-data-pv` → `/mnt/nyc-data`, `project-files-pv` → `/mnt/nyc-project`.
- Custom images built via `docker build` + `kind load docker-image`: `nyc-pipeline-tools:k8s`, `nyc-dbt:k8s`, `nyc-airflow:k8s`.
- **Port mapping conflict**: kind extraPortMappings use `38080-38088`. Use `39080+` range for kubectl port-forward with `--address 0.0.0.0`.
- Services use `ClusterIP` type. No Ingress — access via `kubectl port-forward`.

### Airflow (DAGs)
- **PythonOperator** over BashOperator (more reliable for complex `subprocess.run` calls).
- Docker-in-Docker via `subprocess.run(["docker", ...])` with absolute host paths (`/home/dwcks/vsf_gsm/nyc_new`).
- `capture_output=True, text=True`, logging stdout/stderr. Raises `RuntimeError` on non-zero exit.
- Manual trigger (`schedule=None`), no catchup.

### CDC (Debezium)
- Postgres 16 with WAL logical replication (`wal_level=logical`).
- Debezium Kafka Connect 2.5 — Postgres connector, `ExtractNewRecordState` SMT.
- Bridge script (`scripts/cdc_bridge.py`) reduces JSON envelope to flat format compatible with Spark schema.
- **Async optimization**: Default mode uses `producer.send()` + periodic flush every `--flush-interval` events. Sync mode (`--sync`) forces `producer.send().get()` per event — 280x slower.
- Benchmark: 2,543 ev/s async vs 9 ev/s sync.

---

## Important Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | 16+ services, 6 profiles, 3 named volumes |
| `Makefile` | Single entry point (40+ targets, 9 groups) |
| `kind.yaml` | kind cluster config (3 nodes, port mappings) |
| `jobs/spark_local_batch.py` | Batch backfill — enrichment + validation, writes S3 |
| `jobs/spark_stream_taxi_events.py` | Kafka streaming consumer — same logic as batch |
| `dbt/models/marts/fact_trips.sql` | Primary fact table with derived fields |
| `dbt/models/staging/stg_trips.sql` | Clean column types from silver Parquet |
| `dbt/models/gold/gold_fact_trips.sql` | Gold-level fact with trip_id, source_file |
| `scripts/trino_register.py` | Register Hive tables pointing to S3 paths |
| `scripts/cdc_bridge.py` | CDC topic → standard event format (async) |
| `scripts/superset_bootstrap.py` | Idempotent Superset setup (7 datasets, charts, dashboard) |
| `scripts/run_analytics_questions.py` | 10 SQL analytics queries validated against Trino |
| `docker/tools.Dockerfile` | Base image for all tools containers |
| `docker/dbt.Dockerfile` | dbt-trino runner image |
| `docker/airflow.Dockerfile` | Airflow 2.10.5 image with Docker Compose + providers |
| `docker/trino/etc/catalog/hive.properties` | Hive connector config with S3 endpoint |
| `airflow/dags/nyc_e2e_pipeline.py` | E2E pipeline DAG (spark → trino → dbt → superset) |
| `k8s/trino/configmap.yaml` | Trino config with S3 hive.properties |
| `k8s/jobs/spark-batch-m01.yaml` | Spark batch job manifest (S3 mode, MinIO env vars) |
| `check.md` | Quick reference: UI URLs, credentials, port-forwards, row counts |

---

## Runtime/Tooling Preferences

- **Deployment modes**: Docker Compose (default) or Kubernetes/kind.
- **Docker** is the only runtime requirement for Docker Compose mode. Host needs only Docker + Docker Compose.
- **kind** for local K8s. 3 nodes, hostPath PVCs, NodePort port mappings `38080-38088`.
- **Make** as single entry point (no shell aliases, no manual docker/kubectl commands).
- **Python 3.11** inside containers (tools image), **Spark 3.5.1** (`apache/spark:3.5.1`), **Trino 435**, **dbt-trino 1.11.x**, **Superset 4.0.0**, **Debezium 2.5**, **Airflow 2.10.5**.
- **MinIO** as S3-compatible storage: Spark uses `s3a://`, Trino uses `s3://`.
- **No linter/formatter** configured. Code style is conventional Python.
- **K8s port-forwards**: `--address 0.0.0.0` flag required. Use port range `39080+` (avoid kind NodePort conflict).

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
- Expected: `dim_zone` = 261, `fact_trips` = ~8.48M, `mart_hourly` = ~11K+.

### Full Pipeline (`make verify-all`)
6 steps: Spark batch → Trino bootstrap → dbt build → mart verification → analytics → Superset check → CDC verify.

### Key Constraints
- **Hive HMS**: No `RENAME TABLE`. All dbt models **must** be `materialized='view'`. `materialized='table'` fails at build time.
- **MinIO credentials**: Hardcoded `minio/minio123` in Spark config, Trino catalog, and mc. Change everywhere if rotated.
- **Spark UID mismatch** (Docker): Spark runs as UID 185, host as 1000. `make setup-volumes` fixes data dir permissions (777) — only relevant for local FS mode, not S3.
- **Docker network**: Compose project `nyc_new` creates network `nyc_new_default`. Spark containers need this network to reach MinIO.
- **S3A packages**: Must pass via `--packages` on `spark-submit`, not in SparkSession config.
