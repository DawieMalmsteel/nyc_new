# Repository Guidelines

## Project Overview

NYC Taxi data pipeline — batch + streaming data engineering pipeline running fully in Docker. Ingests NYC TLC trip records (Parquet), processes them with Spark (enrichment + validation), stores silver/quarantine data in Parquet, exposes via Trino (Hive catalog), transforms with dbt-trino into analytics marts, and visualizes via Apache Superset. Also supports Debezium CDC from Postgres → Kafka as an alternative event source.

All operations are driven through **Makefile**; no manual Docker command memorization needed.

---

## Architecture & Data Flow

```
Raw Parquet ──► Spark Batch (local[*]) ──► Silver Parquet ──► Trino (Hive) ──► dbt ──► Superset
      │                                                            ▲              │
      ├── Spark Streaming (Kafka) ──► Silver Parquet ──────────────┘              │
      │                                                                           │
      └── Debezium CDC (Postgres) ──► Kafka ──► cdc_bridge ──► taxi.trip.events  │
                                                                 Airflow (orchestration)
```

**Validation rules** (in Spark streaming & batch):
- `event_id`, `pickup_ts`, `dropoff_ts` must not be null
- `dropoff_ts` > `pickup_ts`
- `trip_distance` > 0, `fare_amount` >= 0, `total_amount` >= `fare_amount`
- `passenger_count` between 1–6
- `pickup_location_id` / `dropoff_location_id` must exist in zone lookup

Valid → `data/silver/trips/` (partitioned by `pickup_year`, `pickup_month`). Invalid → `data/quarantine/invalid_trips/`.

---

## Key Directories

| Directory | Purpose |
|---|---|
| `jobs/` | Spark processors: `spark_local_batch.py` (batch backfill, local[*]), `spark_stream_taxi_events.py` (Kafka streaming) |
| `scripts/` | Utility scripts: CDC (seed, register, bridge), Trino (register, sync partitions), Superset (bootstrap, check), analytics validation, mart verification |
| `airflow/dags/` | DAGs: `nyc_e2e_pipeline` (full pipeline), `nyc_analytics_refresh` (dbt → Superset → analytics) |
| `dbt/` | dbt-trino models (staging → marts) + tests |
| `docker/` | Dockerfiles, entrypoint scripts, Trino configs, Superset configs |
| `data/` | Data lake: `raw/`, `silver/`, `quarantine/`, `lookup/`, `checkpoints/`, `trino-metastore/` (all gitignored) |

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
make spark-batch         # Batch backfill (fast, no Kafka needed)
make spark-streaming     # Submit streaming job to Spark master
```

### Trino
```
make trino-bootstrap     # Register tables from silver parquet
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
- **Type hints** on all function signatures.
- **Config/constants** at module top — named constants in `UPPER_CASE`, schema dicts defined as module-level variables.
- **Docstrings** on modules and functions (triple-quoted, multi-line).
- **Error handling**: `try/except` around external calls (Kafka, API), `log.error` + `raise` on failure. Fail-fast in entrypoints via `set -euo pipefail` (bash).
- **Imports**: stdlib first, then third-party, then local. No `__init__.py` re-exports.
- **Main guard**: `if __name__ == "__main__": main()` pattern.

### Spark (PySpark)
- `SparkSession.builder.appName(...)` with `local[*]` master for batch, cluster `spark://spark-master:7077` for streaming.
- Schemas defined as `StructType([StructField(...)])` lists, not DDL strings.
- Transformations use `spark.sql.functions` (not raw SQL in streaming).
- Column expressions via `col(...)`.
- Zone lookup join via CSV → broadcast-style (small lookup, directly joined).
- Stream processing uses `foreachBatch` + `writeStream.trigger(availableNow=True)` for batch-mode consumption.
- Output partitioned by `pickup_year`, `pickup_month`.

### dbt (SQL)
- **Naming**: `stg_` (staging), `dim_` (dimension), `fact_` (fact), `mart_` (summary).
- **Materialization**: All models are `view` (Hive file-based HMS does not support `RENAME TABLE` which dbt uses for table swaps). Never use `materialized='table'`.
- **Test files**: YAML per model (`stg_trips_tests.yml`, `fact_trips_tests.yml`) with `not_null`, `accepted_values` generic tests. Singular tests as `.sql` files (e.g., `payment_type_range.sql`).
- **Refs**: Models reference each other via `{{ ref('model_name') }}`. No direct table references across layers.

### Airflow (DAGs)
- **PythonOperator** over BashOperator (more reliable for complex `subprocess.run` calls).
- Docker-in-Docker via `subprocess.run(["docker", ...])` with absolute host paths (`/home/dwcks/vsf_gsm/nyc_new`).
- `capture_output=True, text=True`, logging stdout/stderr. Raises `RuntimeError` on non-zero exit.
- Manual trigger (`schedule=None`), no catchup.

### Docker Compose
- **Profiles** for service grouping: `tools`, `trino`, `dbt`, `superset`, `airflow`.
- One-shot services (`restart: "no"`) vs daemon services (`restart: unless-stopped`).
- Tools image (`nyc-pipeline-tools:latest`) built from `docker/tools.Dockerfile` — base Python 3.10, includes `kafka-python`, `psycopg2-binary`, `pyarrow`, `pandas`, `sqlalchemy`.
- Entrypoint scripts in `docker/` called via `command:` or `entrypoint:` in service definition.
- `group_add: ["958"]` on Airflow services for Docker socket access.

---

## Important Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | All 18 services, 6 profiles, 3 named volumes |
| `Makefile` | Single entry point (40+ targets, 9 groups) |
| `jobs/spark_local_batch.py` | Batch backfill — full enrichment + validation |
| `jobs/spark_stream_taxi_events.py` | Kafka streaming consumer — same logic as batch |
| `dbt/models/staging/stg_trips.sql` | Clean column types from silver Parquet |
| `dbt/models/marts/fact_trips.sql` | Primary fact table with derived fields |
| `dbt/models/marts/mart_hourly_summary.sql` | Hourly aggregations |
| `scripts/cdc_bridge.py` | CDC topic → standard event format |
| `scripts/superset_bootstrap.py` | Idempotent Superset setup (DB, dataset, charts, dashboard) |
| `scripts/run_analytics_questions.py` | 10 SQL analytics queries validated against Trino |
| `docker/entrypoint-airflow.sh` | Role-based Airflow entrypoint (webserver/scheduler/init) |
| `docker/tools.Dockerfile` | Base image for all tools containers |

---

## Runtime/Tooling Preferences

- **Docker** is the only runtime requirement. All code runs in containers. Host needs only Docker + Docker Compose.
- **Make** as single entry point (no shell aliases, no manual `docker compose` command memorization).
- **Python 3.10+** inside containers (tools image), **Spark 3.5.1** (`apache/spark:3.5.1`), **Trino 435**, **dbt-trino 1.10.2**, **Superset 4.0.0**, **Debezium 2.5**.
- **No host Python** except for running `make verify-mart`/`verify-analytics` (small local scripts) — but can also run inside Docker.
- **No linter/formatter** configured (pre-commit not set up). Code style is conventional Python.
- **Commit style**: Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`), imperative mood.

---

## Testing & QA

### dbt Tests (run via `make dbt-build`)
- 6 models, 9 tests — 15/15 pass expected.
- **Generic tests**: `not_null`, `accepted_values` in YAML test files.
- **Singular tests**: Custom SQL queries in `dbt/tests/`.
- Tests cover: NOT NULL on key columns (total_amount, pickup_ts, dropoff_ts, payment_type, trip_distance), accepted values for payment_type (1–6), payment_type range sanity.

### Analytics Validation (`make verify-analytics`)
- 10 SQL questions run against Trino via `scripts/run_analytics_questions.py`.
- Each query must return a result; PASS/FAIL per question printed.
- Expect 10/10 PASS.

### Mart Verification (`make verify-mart`)
- Row counts: `dim_zone` = 261, `fact_trips` = ~2.7M (batch), `mart_hourly` = ~3945.

### Full Pipeline (`make verify-all`)
6 steps: Spark batch → Trino bootstrap → dbt build → mart verification → analytics → Superset check.

### Key Constraints
- **Hive HMS limitation**: No `RENAME TABLE`. All dbt models must be `materialized='view'`.
- **Spark UID mismatch**: In Docker, Spark runs as UID 185, host is UID 1000. Run `make setup-volumes` to set data directories to 777.
- **Superset Trino dialect**: Requires `sqlalchemy-trino` pip installed in the container (done in entrypoint).
