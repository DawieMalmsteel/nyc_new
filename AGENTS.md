# Repository Guidelines

## Project Overview

Local-first, Dockerized NYC Taxi data pipeline (MVP). Ingests NYC TLC yellow taxi trip data via Kafka, processes with Spark (batch & streaming), stores as Parquet on local filesystem, and serves analytics through Trino + dbt models with visualization in Apache Superset. Orchestrated by Apache Airflow.

**Entry point:** `Makefile` — all operations via `make <target>`.

---

## Architecture & Data Flow

```
Raw Parquet → Generator → Kafka → Spark Streaming → Parquet (silver/quarantine)
                                                          ↓
Batch Backfill → Parquet (silver) —─────────────────→ Trino (Hive catalog)
                                                          ↓
                                                     dbt (views)
                                                          ↓
                                              Superset ←──┘
                                              Airflow (orchestrates all)
```

**Two ingestion paths:**

1. **Batch** (fast, offline): `make spark-batch` — reads raw Parquet directly, enriches with zone lookup, writes silver/quarantine.
2. **Streaming** (Kafka): `generator/` → Kafka topic `taxi.trip.events` → `jobs/spark_stream_taxi_events.py` (Spark Structured Streaming).

**Analytics layer:**

- Trino queries Parquet via Hive connector (file-based HMS).
- dbt-trino builds views in `hive.mart` schema.
- Superset connects to Trino via `sqlalchemy-trino` dialect.

---

## Key Directories

| Directory | Purpose |
|---|---|
| `jobs/` | Spark batch/streaming processors, Kafka fallback processor, quality report |
| `generator/` | Kafka event generator reading raw Parquet files |
| `airflow/dags/` | Airflow DAGs (`nyc_analytics_refresh`, `nyc_e2e_pipeline`) |
| `scripts/` | Utility scripts: verify, bootstrap, analytics queries, Trino registration |
| `dbt/models/` | dbt models: `staging/` (stg_trips, stg_invalid_trips), `marts/` (dim_zone, fact_trips, fact_invalid_trips, mart_hourly_summary) |
| `dbt/tests/` | dbt data tests (9 tests: not_null checks, payment_type_range, etc.) |
| `docker/` | Dockerfiles, entrypoints, Trino config, Superset bootstrap |
| `sql/` | Raw SQL queries for analytics validation and smoke tests |
| `data/` | Pipeline data (gitignored): raw, silver, quarantine, checkpoints, trino-metastore |

---

## Development Commands

All operations use `make` — no need to memorize docker commands.

### Infrastructure
```bash
make infra-up           # Start core: ZK, Kafka, Kafka-UI, MinIO, Spark
make infra-up-all       # Start everything (incl. Trino, dbt, Superset, Airflow)
make infra-logs SVC=X   # Tail logs for a service
```

### Data Pipeline
```bash
make kafka-publish               # Publish 5000 events to Kafka
make spark-batch                 # Batch backfill (2.7M rows, ~30s)
make spark-streaming             # Submit streaming job to Spark master
make trino-bootstrap             # Register tables + sync partitions
make dbt-build                   # dbt models + tests
```

### Analytics & Visualization
```bash
make superset-bootstrap          # Idempotent bootstrap (DB, dataset, 4 charts, dashboard)
make superset-check              # List Superset resources
make verify-mart                 # Row counts of all mart tables
make verify-analytics            # Run 10 SQL analytics questions
```

### Verification
```bash
make verify-all                  # Full pipeline: batch → Trino → dbt → analytics → Superset
make verify-e2e                  # Full Kafka E2E test (~5000 events)
```

### UI Access
| Service | URL | Credentials |
|---|---|---|
| Kafka UI | `http://localhost:8080` | — |
| Trino | `http://localhost:8083` | — |
| Superset | `http://localhost:8088` | `admin/admin` |
| Airflow | `http://localhost:8085` | `admin/admin` |
| MinIO Console | `http://localhost:9001` | `minio/minio123` |

---

## Code Conventions & Common Patterns

### Spark Jobs (`jobs/`)
- Use `SparkSession.builder.appName(...)`.
- Accept paths as CLI arguments via `argparse`.
- **Validation pattern**: filter valid/invalid records using `when()` conditions collected in an `array()`, then `filter()` out nulls. `is_valid = size(validation_errors) == 0`.
- **Write pattern**: `mode("append")` with explicit `partitionBy` for silver; plain `parquet` for quarantine. Persist/unpersist batch in `forEachBatch`.
- UID 185 inside Docker (Spark), UID 1000 on host. Dirs must be 777. Use `make setup-volumes` to fix.

### dbt Models
- All models materialized as `view` (Hive file-based HMS does not support `RENAME TABLE`).
- Profile: `nyc_taxi`, target `dev`, connector `trino`, schema `mart`.
- Model hierarchy: staging views → mart views → tests.
- Marts: `dim_zone`, `fact_trips`, `fact_invalid_trips`, `mart_hourly_summary`.

### Python Scripts (`scripts/`)
- Direct Trino connections via `trino.dbapi.connect` (`localhost:8083`).
- Superset API calls use `urllib.request` with JWT auth from `POST /security/login`.
- Scripts are idempotent (check-then-create).

### Airflow DAGs
- `PythonOperator` calling `subprocess.run` for Docker CLI commands.
- Docker-in-Docker uses absolute host paths (`/home/dwcks/vsf_gsm/nyc_new`) because bind-mount sources resolve on host, not inside container.
- Uses `LocalExecutor`, Postgres metadata backend.

### Docker Compose
- Profile-based service grouping: `tools`, `trino`, `dbt`, `superset`, `airflow`.
- Volumes: `./` (project root) mounted as `/opt/project` in containers.
- Airflow containers mount `/var/run/docker.sock` for Docker access.

---

## Important Files

| File | Role |
|---|---|
| `Makefile` | Single entry point for all operations (30+ targets) |
| `docker-compose.yml` | All service definitions with profiles |
| `jobs/spark_local_batch.py` | Batch processor with full enrichment and validation |
| `jobs/spark_stream_taxi_events.py` | Kafka → Silver streaming processor |
| `generator/taxi_event_generator.py` | Parquet → Kafka event producer with invalid injection |
| `airflow/dags/nyc_analytics_refresh.py` | DAG: dbt build → Superset bootstrap → analytics check |
| `dbt/profiles.yml` | dbt Trino connection config |
| `scripts/superset_bootstrap.py` | Idempotent Python Superset bootstrap (DB, dataset, 4 charts, 1 dashboard) |
| `scripts/verify_mart.py` | Row count verification across all mart tables |
| `scripts/run_analytics_questions.py` | 10 SQL analytics questions validation |
| `sql/analytics_questions.sql` | SQL queries for analytics validation |
| `docker/trino/etc/catalog/hive.properties` | Trino Hive connector config |

---

## Runtime/Tooling Preferences

- **Docker** is the primary runtime. All pipeline components (Spark, Trino, dbt, Superset, Airflow) run in containers. No host Python dependencies beyond `docker compose`.
- **Make** is the entry point. Never invoke docker compose commands directly without `make`.
- **Spark**: `apache/spark:3.5.1` image. Batch uses `local[*]` mode directly (no cluster needed). Streaming submits to `spark-master:7077`.
- **Trino**: `trinodb/trino:435`, Hive connector with file-based HMS at `data/trino-metastore/`.
- **dbt**: `dbt-trino` 1.10.2, models materialized as `view`. Connection via docker network to `trino-coordinator:8080`.
- **Superset**: `apache/superset:4.0.0`, requires `sqlalchemy-trino` pip package (installed in entrypoint).
- **Airflow**: Postgres metadata, `LocalExecutor`, Docker group GID `958` needed for Docker-in-Docker.

---

## Testing & QA

### dbt Tests
- 9 data tests across staging and mart models.
- Run with `make dbt-build` (includes dbt `build` which runs models + tests).
- Expected: `PASS=15 WARN=0 ERROR=0 SKIP=0`.
- Cannot use `materialized='table'` due to Hive HMS rename limitation.

### Analytics Validation
- 10 SQL questions in `sql/analytics_questions.sql`.
- Run via `make verify-analytics` → script queries Trino directly.
- Expected: `PASS 10/10`.

### Mart Verification
- `make verify-mart` prints row counts: `dim_zone` (~261), `fact_trips` (~2.7M), `mart_hourly_summary` (~3945).

### Full Pipeline
- `make verify-all` runs all 6 stages and reports pass/fail.
- Scenarios for regression:
  1. Spark batch writes 0 valid trips → check zone lookup CSV exists.
  2. dbt fails with `PERMISSION_DENIED` rename → ensure all marts are `view`, not `table`.
  3. Trino sees 0 rows → re-run `make trino-bootstrap` and `make setup-volumes`.
  4. Superset can't connect → ensure `sqlalchemy-trino` installed in Superset container.
