# Repository Guidelines

## Project Overview

NYC Taxi data pipeline — batch + streaming data engineering pipeline running fully in Docker. Ingests NYC TLC trip records (Parquet), processes them with Spark (enrichment + validation), stores silver/quarantine data in MinIO S3, exposes via Trino (Hive catalog), transforms with dbt-trino into analytics marts, and visualizes via Apache Superset. Also supports Debezium CDC from Postgres → Kafka as an alternative event source.

All operations are driven through **Makefile**; no manual Docker command memorization needed.

---

## Architecture & Data Flow

```
Raw Parquet ──► Spark Batch (local[*]) ──► Silver (MinIO S3) ──► Trino (Hive) ──► dbt ──► Superset
      │                                                          ▲              │
      ├── Spark Streaming (Kafka) ──► Silver (MinIO S3) ─────────┘              │
      │                                                                         │
      └── Debezium CDC (Postgres) ──► Kafka ──► cdc_bridge ──► taxi.trip.events│
                                                                                │
                    Airflow (orchestration) ─────────────────────────────────────┘
```

**Validation rules** (in Spark streaming & batch):
- `event_id`, `pickup_ts`, `dropoff_ts` must not be null
- `dropoff_ts` > `pickup_ts`
- `trip_distance` > 0, `fare_amount` >= 0, `total_amount` >= `fare_amount`
- `passenger_count` between 1–6
- `pickup_location_id` / `dropoff_location_id` must exist in zone lookup

Valid → `s3a://nyc-silver/trips/` (partitioned by `pickup_year`, `pickup_month`). Invalid → `s3a://nyc-quarantine/invalid_trips/`.

**Storage backends:**
- MinIO S3 (`s3a://`) for all pipeline data (raw, silver, quarantine, lookup)
- Local filesystem for Hive metastore and streaming checkpoints

---

## Key Directories

|Directory|Purpose|
|---|---|
|`jobs/`|Spark processors: `spark_local_batch.py` (batch backfill, `local[*]`), `spark_stream_taxi_events.py` (Kafka streaming)|
|`scripts/`|Utility scripts: CDC (seed, register, bridge), Trino (register, sync partitions), Superset (bootstrap, check), analytics validation, mart verification, data download|
|`airflow/dags/`|DAGs: `nyc_e2e_pipeline` (full pipeline), `nyc_analytics_refresh` (dbt → Superset → analytics)|
|`dbt/`|dbt-trino models (staging → marts → gold) + tests|
|`docker/`|Dockerfiles, entrypoint scripts, Trino configs, Superset configs|
|`data/`|Data lake: `raw/`, `silver/`, `quarantine/`, `lookup/`, `checkpoints/`, `trino-metastore/` (all gitignored)|
|`k8s/`|Kubernetes manifests (kind cluster alternate deployment)|
|`sql/`|Analytics SQL questions for validation (`analytics_questions.sql`)|
|`generator/`|Kafka event generator (Python)|

---

## Development Commands

All operations via `make <target>`. Makefile structure (9 groups):

### Infrastructure
```
make infra-up            # Start core: ZK, Kafka, Kafka-UI, MinIO, Spark
make infra-up-all        # Everything (core + Trino + dbt + Superset + Airflow)
make infra-status        # docker compose ps
make infra-logs SVC=trino # Tail logs for a service
```

### Kafka
```
make kafka-topics        # Create topics (taxi.trip.events, .invalid, .dlq)
make kafka-publish       # Publish events (default 5000, via generator)
make kafka-publish-full  # All 9.5M events (hours)
```

### CDC (Debezium)
```
make cdc-up              # Start Postgres + Debezium
make cdc-seed            # Seed Postgres from parquet (5000 rows)
make cdc-register        # Register Debezium connector
make cdc-bridge          # Bridge CDC topic → taxi.trip.events
make cdc-verify          # Full CDC E2E
```

### Spark
```
make spark-batch         # Batch backfill via MinIO S3 (fast, no Kafka needed)
MONTH=03 make spark-batch  # Specific month
make spark-streaming     # Submit streaming job to Spark master
```

### Trino
```
make trino-bootstrap     # Register tables from silver parquet (idempotent)
make trino-shell         # Interactive Trino shell
```

### dbt
```
make dbt-build           # Full dbt build (models + tests)
make dbt-run             # Models only
make dbt-test            # Tests only
```

### Superset
```
make superset-bootstrap  # Register DB, dataset, 4 charts, dashboard (idempotent)
make superset-check      # List resources
# UI at http://localhost:8088 (admin/admin)
```

### Airflow
```
make airflow-up          # Start Airflow (after infra-up)
make airflow-trigger DAG=nyc_analytics_refresh
# UI at http://localhost:8085 (admin/admin)
```

### Verify & Clean
```
make verify-all          # Full pipeline: batch → Trino → dbt → analytics → Superset
make verify-mart         # Row counts in Trino
make verify-analytics    # 10 SQL questions (expect PASS 10/10)
make clean-all           # Delete generated data
make setup-volumes       # Fix data dir permissions (777)
```

---

## Code Conventions & Common Patterns

### Python
- **argparse** for CLI (no click/typer). All scripts use `parser.add_argument()` with typed defaults.
- **Type hints** on all function signatures, return types annotated.
- **Config/constants** at module top — named constants in `UPPER_CASE`, schema dicts defined as module-level variables.
- **Docstrings** on modules and functions (triple-quoted, multi-line).
- **Error handling**: `try/except` around external calls (Kafka, REST API), `log.error` + `raise` on failure. Fail-fast in entrypoints via `set -euo pipefail` (bash).
- **Imports**: stdlib first, then third-party, then local. No `__init__.py` re-exports.
- **Main guard**: `if __name__ == "__main__": main()` pattern with `sys.exit(main())` for CLI return codes.
- **Entrypoint scripts** in `docker/` — minimal bash wrappers that delegate to Python; always use `set -euo pipefail` and `exec`.

### Spark (PySpark)
- `SparkSession.builder.appName(...)` with `local[*]` master for batch, cluster `spark://spark-master:7077` for streaming.
- Schemas defined as `StructType([StructField(...)])` lists, not DDL strings.
- Transformations use `spark.sql.functions` (not raw SQL in streaming).
- Column expressions via `col(...)`.
- Zone lookup join via broadcast-style (small lookup, directly joined without explicit broadcast hint).
- Stream processing uses `foreachBatch` + `writeStream.trigger(availableNow=True)` for batch-mode consumption.
- Output partitioned by `pickup_year`, `pickup_month`.
- MinIO S3 config via `spark.hadoop.fs.s3a.*` with hardcoded credentials (`minio/minio123`).

### dbt (SQL)
- **Naming**: `stg_` (staging), `dim_` (dimension), `fact_` (fact), `gold_` (gold layer), `mart_` (summary).
- **Materialization**: All models are `view` (Hive file-based HMS does not support `RENAME TABLE` which dbt uses for table swaps). Never use `materialized='table'`.
- **Model layers**: 3 staging models, 3 marts (fact + dim + summaries), 4 gold models, 5 additional marts. Staging reads from `hive.nyc.*` directly; marts reference staging via `{{ ref('stg_trips') }}`.
- **Test files**: YAML per model section (`stg_trips_tests.yml`, `fact_trips_tests.yml`, `fact_invalid_trips_tests.yml`) with `not_null`, `accepted_values` generic tests. Singular tests as `.sql` files (e.g., `payment_type_range.sql`).
- **Refs**: Models reference each other via `{{ ref('model_name') }}`. No direct table references across layers.
- **Derived fields**: `tip_rate = tip_amount / total_amount`, `trip_duration_sec` via `date_diff`.

### Airflow (DAGs)
- **PythonOperator** over BashOperator (more reliable for complex `subprocess.run` calls).
- Docker-in-Docker via `subprocess.run(["docker", ...])` with absolute host paths (`/home/dwcks/vsf_gsm/nyc_new`).
- `capture_output=True, text=True`, logging stdout/stderr. Raises `RuntimeError` on non-zero exit.
- Manual trigger (`schedule=None`), no catchup.

### Docker Compose
- **Profiles** for service grouping: `tools`, `trino`, `dbt`, `superset`, `airflow`.
- One-shot services (`restart: "no"`) vs daemon services (`restart: unless-stopped`).
- Tools image (`nyc-pipeline-tools:latest`) built from `docker/tools.Dockerfile` — base Python 3.11, includes `kafka-python`, `psycopg2-binary`, `pyarrow`, `pandas`, `sqlalchemy`.
- Entrypoint scripts in `docker/` called via `command:` or `entrypoint:` in service definition.
- MinIO S3 credentials hardcoded: `minio` / `minio123`.

---

## Important Files

|File|Purpose|
|---|---|
|`docker-compose.yml`|All 16+ services, 6 profiles, 3 named volumes|
|`Makefile`|Single entry point (40+ targets, 9 groups)|
|`jobs/spark_local_batch.py`|Batch backfill — full enrichment + validation, writes to S3|
|`jobs/spark_stream_taxi_events.py`|Kafka streaming consumer — same enrichment logic as batch|
|`dbt/models/staging/stg_trips.sql`|Clean column types from silver Parquet|
|`dbt/models/marts/fact_trips.sql`|Primary fact table with derived fields|
|`dbt/models/marts/mart_hourly_summary.sql`|Hourly aggregations|
|`dbt/models/gold/gold_fact_trips.sql`|Gold-level fact with trip_id, source_file tracking|
|`scripts/trino_register.py`|Register Hive tables pointing to S3 paths|
|`scripts/cdc_bridge.py`|CDC topic → standard event format|
|`scripts/superset_bootstrap.py`|Idempotent Superset setup (DB, dataset, charts, dashboard)|
|`scripts/run_analytics_questions.py`|10 SQL analytics queries validated against Trino|
|`scripts/download_data.sh`|Download raw parquet + zone lookup from NYC TLC|
|`docker/tools.Dockerfile`|Base image for all tools containers|
|`docker/dbt.Dockerfile`|dbt-trino runner image|
|`docker/airflow.Dockerfile`|Airflow 2.10.5 image with Docker Compose + providers|
|`docker/trino/etc/catalog/hive.properties`|Hive connector config with S3 endpoint|
|`airflow/dags/nyc_e2e_pipeline.py`|E2E pipeline DAG (spark → trino → dbt → superset)|

---

## Runtime/Tooling Preferences

- **Docker** is the only runtime requirement. All code runs in containers. Host needs only Docker + Docker Compose.
- **Make** as single entry point (no shell aliases, no manual `docker compose` command memorization).
- **Python 3.11** inside containers (tools image), **Spark 3.5.1** (`apache/spark:3.5.1`), **Trino 435**, **dbt-trino 1.11.x**, **Superset 4.0.0**, **Debezium 2.5**, **Airflow 2.10.5**.
- **No host Python** except for running `make verify-mart`/`verify-analytics` (small local scripts connecting to Trino on `localhost:8083`).
- **MinIO** as S3-compatible storage: endpoints `http://minio:9000` (internal), `http://localhost:9000` (external), console at `http://localhost:9001`.
- **No linter/formatter** configured (pre-commit not set up). Code style is conventional Python.

---

## Testing & QA

### dbt Tests (run via `make dbt-build`)
- 15 models, 9 data tests — **24/24 PASS** expected.
- **Generic tests**: `not_null`, `accepted_values` in YAML test files.
- **Singular tests**: Custom SQL queries in `dbt/tests/`.
- Tests cover: NOT NULL on key columns (total_amount, pickup_ts, dropoff_ts, payment_type, trip_distance), accepted values for payment_type (1–6), payment_type range sanity.

### Analytics Validation (`make verify-analytics`)
- 10 SQL questions run against Trino via `scripts/run_analytics_questions.py` from `sql/analytics_questions.sql`.
- Each query must return at least 1 row; PASS/FAIL per question printed.
- Expect **10/10 PASS**.

### Mart Verification (`make verify-mart`)
- Row counts verified against Trino `hive.mart.*` views.
- Expected: `dim_zone` = 261, `fact_trips` = ~8.4M (3 months batch), `mart_hourly` = ~11K+ (varies by data).

### Full Pipeline (`make verify-all`)
6 steps: Spark batch → Trino bootstrap → dbt build → mart verification → analytics → Superset check → CDC verify.

### Key Constraints
- **Hive HMS limitation**: No `RENAME TABLE`. All dbt models must be `materialized='view'`. Using `materialized='table'` will fail at dbt build time.
- **MinIO S3 credentials**: Hardcoded `minio/minio123` across spark config, trino catalog, and mc client. Change in all places if rotated.
- **Spark UID mismatch**: In Docker, Spark runs as UID 185, host is UID 1000. Run `make setup-volumes` to set data directories to 777 (only relevant for local FS mode, not S3).
- **Network name**: Compose project name `nyc_new` creates network `nyc_new_default`. Spark containers need `--network nyc_new_default` to reach MinIO.
