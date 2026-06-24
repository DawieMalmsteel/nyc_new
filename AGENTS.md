# Repository Guidelines

## Project Overview

NYC Taxi data pipeline — batch + streaming data engineering project. Ingests NYC TLC yellow taxi trip records (Parquet), enriches and validates them in Spark, stores silver/quarantine data in MinIO S3, exposes them through Trino (Hive catalog over S3), transforms with dbt-trino (3-layer model hierarchy: staging → marts → gold), and visualizes via Apache Superset. Also supports a parallel CDC path: Debezium reads Postgres WAL → Kafka → Spark streaming, all wired to the same MinIO + Trino + dbt + Superset downstream.

On Kubernetes (kind + Skaffold + Helm) Airflow is the orchestrator with three DAGs: `nyc_e2e_pipeline` (monthly), `nyc_analytics_refresh` (weekly), `nyc_cdc_pipeline` (manual). K8s + Skaffold is the primary deployment target. Docker Compose + Makefile is the local-dev path.

## Architecture & Data Flow

```
                  MinIO S3 (s3a://)
                 ┌─────┴──────┬──────────────┐
   raw parquet   │  silver/   │  quarantine/ │  gold/
   (nyc-raw)     │  (trips)   │  invalid     │  (33 datasets)
                 └─────┬──────┴──────┬───────┴──────┬────────┘
                       │             │              │
                       ▼             ▼              ▼
                  Trino Hive catalog              Postgres analytics
                  (hive.nyc, hive.mart,          (nyc_analytics.public)
                   hive.nyc_gold)                       ▲
                       │                              │
                       ▼                              │
                  dbt-trino (15 models, ──── materialize_to_postgres.py
                  staging→marts→gold)              (gold layer copy)
                                                       ▲
                  export_gold_to_minio.py            │
                  (CTAS Parquet → s3://nyc-gold/) ───┘
                                          │
                                          ▼
                                    Superset
```

Two ingestion paths:
- **Batch**: `jobs/spark_local_batch.py` reads `s3a://nyc-raw/yellow_taxi/year=YYYY/month=MM/*.parquet`
- **Streaming**: `jobs/spark_stream_taxi_events.py` reads Kafka topic `taxi.trip.events`

Both enrich + validate with 10 rules, then write valid → `s3a://nyc-silver/trips` (partitioned by `pickup_year`, `pickup_month`) and invalid → `s3a://nyc-quarantine/invalid_trips`.

**Validation rules** (in both Spark batch and streaming):
- `pickup_ts`, `dropoff_ts` must not be null
- `dropoff_ts > pickup_ts`
- `trip_distance > 0`, `fare_amount >= 0`, `total_amount >= fare_amount`
- `passenger_count` between 1–6
- `payment_type` between 1–6
- `pickup_location_id` / `dropoff_location_id` must exist in zone lookup

**Spark zone cleaning** (at source in both jobs): `F.when(F.col("Borough").isin("Unknown","N/A","NV"), F.lit(None))` etc. This handles NYC TLC zone IDs 264 (Unknown/N/A) and 265 (N/A/Outside of NYC). Belt-and-suspenders `nullif()` in dbt stg_trips.sql.

## Key Directories

| Directory | Purpose |
|---|---|
| `jobs/` | Spark processors: `spark_local_batch.py` (batch), `spark_stream_taxi_events.py` (Kafka streaming) |
| `scripts/` | Trino bootstrap, partition sync, gold export, Postgres materialize, Superset setup, CDC bridge, verify |
| `dbt/` | dbt-trino project: `models/staging/`, `models/marts/`, `models/gold/`, `tests/`, `profiles.yml`, `dbt_project.yml` |
| `airflow/dags/` | DAGs: `nyc_e2e_pipeline.py`, `nyc_analytics_refresh.py`, `nyc_cdc_pipeline.py` |
| `charts/nyc-taxi/` | Helm chart for K8s (all service manifests) |
| `docker/` | Dockerfiles + entrypoint scripts (`.sh` wrappers calling Python) |
| `data/` | Data lake (gitignored): raw parquet, lookup CSV, Trino metastore DB files |
| `docs/` | 14 Vietnamese markdown docs (architecture, deployment, Spark, dbt, Trino, Airflow, CDC, Superset, Docker, Helm/Skaffold, scripts, data flow) |
| `k8s/` | ⛔ DEPRECATED — frozen, stale, do not use. Superseded by `charts/nyc-taxi/`. See `k8s/README.md` for list of known breakage |
| `sql/` | `analytics_questions.sql` (10 verification queries), `smoke_tests.sql` |
| `reports/` | Data quality and verification reports |
| `terraform/` | Terraform configs for MinIO bucket management |

## Development Commands

### Kubernetes / Skaffold (primary)

```bash
# Single entry point — builds images, deploys Helm, port-forwards, watches
skaffold dev --namespace nyc-taxi
```

Skaffold pipeline: build → pre-deploy hook (tar-sync code to PVC, load `:latest` images to kind nodes) → Helm install/upgrade → port-forwards. Pre-deploy hook is **minimal** — no namespace clean, no data sync (handled by `scripts/cluster_up.sh`).

```bash
# First time setup (run once):
bash scripts/cluster_up.sh          # kind cluster + public images + .ivy2 cache

# Deploy:
skaffold dev --namespace nyc-taxi   # builds images, syncs code, deploys, watches

# Reset when cluster is in bad state:
bash scripts/reset_ns.sh            # force-clean namespace + release PVs
```

- `kubectl get pods -n nyc-taxi` — check pod status
- `kubectl logs -n nyc-taxi <pod>` — tail pod logs
- `kubectl exec -it -n nyc-taxi <pod> -- bash` — exec into pod
- `kubectl delete namespace nyc-taxi --force --grace-period=0` — force-clean stuck namespace

### Docker Compose (local dev / test)

```bash
make infra-up          # ZK + Kafka + MinIO + Spark
make kafka-topics
make spark-batch [MONTH=03]
make trino-bootstrap
make dbt-build
make gold-export
make superset-bootstrap
make verify-all        # 7-step E2E
make clean-all
```

### UIs / Port-forwards

K8s uses range 39080+:
| Port | Service | URL |
|---|---|---|
| 39080 | Superset | `http://localhost:39080` (admin/admin) |
| 39081 | MinIO API | `http://localhost:39081` |
| 39082 | Kafka UI | `http://localhost:39082` |
| 39083 | Spark Master | `http://localhost:39083` |
| 39084 | Trino | `http://localhost:39084` |
| 39085 | Airflow | `http://localhost:39085` (admin/admin) |
| 39086 | MinIO Console | `http://localhost:39086` (minio/minio123) |
| 39087 | Postgres CDC | `http://localhost:39087` |

## Code Conventions & Common Patterns

### Python (Spark, scripts, entrypoints)

- `argparse` for CLI (no click/typer). Scripts use typed defaults.
- Type hints on function signatures, return types annotated.
- Module-level constants in `UPPER_CASE`, schema dicts as module-level variables.
- `if __name__ == "__main__": main()` + `sys.exit(main())` pattern.
- Trino client: `trino.dbapi.connect(host, port, user)` with `cur.execute` + `cur.fetchall`. `TrinoUserError` is the dominant catch target. **Paramstyle is `qmark` (use `?` not `%s`).**
- Every bootstrap script has `wait_for_<dep>(host, port, timeout=300)` polling `SELECT 1` with 2s backoff.
- Lazy pip install for optional deps: `minio` is installed at runtime if missing.

### Spark (PySpark)

- `SparkSession.builder` with `local[*]` master for batch, `spark://spark-master:7077` for streaming.
- Schemas as `StructType([StructField(...)])` lists, not DDL strings.
- Transformations via `spark.sql.functions` (not raw SQL in streaming).
- Zone cleanup: `F.when(F.col("Borough").isin("Unknown","N/A","NV"), F.lit(None)).otherwise(...)` — NOT `nullif(nullif(...))` which is invalid PySpark.
- Output partitioned by `pickup_year, pickup_month`.
- `mode("append")` (never `overwrite`) to avoid data loss.
- `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` for MinIO.
- S3A JARs loaded via `--jars` (not `--packages`) in K8s to avoid Maven downloads (pod has no internet). JARs synced to PVC one-time by `cluster_up.sh`.
- Ivy cache on PVC: `spark.jars.ivy=/opt/project/.ivy2`.
- `wait_for_minio()` before reading data — polls `/minio/health/live` up to 120s.
- `--incremental` flag: reads max(pickup_year, pickup_month) from silver, only processes newer partitions.

### dbt (SQL)

- All models `materialized=view` (Hive HMS doesn't support `RENAME TABLE`).
- Naming: `stg_` (staging), `dim_`/`fact_`/`mart_` (marts), `gold_` (gold layer — 14 BI models).
- `nullif()` for cleaning N/A/Unknown/NV zone values: `nullif(nullif(col, 'N/A'), 'NV')`.
- Models reference each other via `{{ ref('model_name') }}`.
- dbt target: `dev` (Trino, views in `hive.mart`) or `postgres_analytics` (Postgres, gold models as tables).

### Docker Compose

- `set -euo pipefail` and `exec` in all entrypoints.
- Profile groups: `default` (core), `tools`, `trino`, `dbt`, `superset`, `airflow`.
- MinIO credentials hardcoded `minio/minio123` everywhere.

### Kubernetes (kind + Skaffold)

- kind cluster: 1 control-plane + 2 workers, both workers mount host repo at `/mnt/nyc-project` and `data/` at `/mnt/nyc-data` (hostPath RWO PVCs).
- Service naming: `svc-` prefix (e.g., `svc-trino`, `svc-postgres-analytics`).
- Spark streaming task uses `svc-kafka:9092` (not `kafka:9092`) — K8s service DNS.
- Airflow uses `KubernetesPodOperator` (not BashOperator). Pods mount `project-files-pvc` at `/opt/project`. SA: `airflow-sa`.
- Skaffold pre-deploy hook: tar-sync code to PVC, load `:latest` images to all 3 kind nodes (docker save → tee → ctr import).
- DAG tasks use `image: nyc-pipeline-tools:latest` with `imagePullPolicy: IfNotPresent`.
- Port-forwards managed by Skaffold. If conflicts occur, kill old port-forwards: `pkill -f "port-forward"`.
- If namespace stuck in `Terminating`: `kubectl delete namespace nyc-taxi --force --grace-period=0`.

## Important Files

| File | Purpose |
|---|---|
| `kind.yaml.template` | kind cluster spec with `${PWD}` — generates `kind.yaml` on `cluster_up.sh`; includes kubeadmConfigPatches for API QPS |
| `kind.yaml` | Generated per-machine (gitignored) |
| `skaffold.yaml` | 4 image artifacts, Helm deploy, minimal pre-deploy hook, port-forwards |
| `.dockerignore` | Excludes data/, .ivy2/, .git from build context |
| `scripts/cluster_up.sh` | One-shot: generates kind.yaml + creates cluster + loads images + syncs .ivy2 |
| `scripts/setup_kind_images.sh` | Pulls 10 public images + loads to all 3 kind nodes (checks skip, no `kind load`) |
| `scripts/reset_ns.sh` | Force-clean namespace + release PVs when cluster is in bad state |
| `docker-compose.yml` | 16+ services, 6 profiles |
| `Makefile` | Compose-mode entry point (9 target groups) + K8s targets |
| `dbt/dbt_project.yml` | dbt project config (views default, gold=table in postgres_analytics) |
| `dbt/profiles.yml` | `dev` (Trino) + `postgres_analytics` (Postgres) targets |
| `jobs/spark_local_batch.py` | Spark batch: read raw → enrich → validate → silver/quarantine |
| `jobs/spark_stream_taxi_events.py` | Spark Kafka consumer, same enrichment+validation |
| `scripts/trino_register.py` | Idempotent Trino catalog bootstrap |
| `scripts/export_gold_to_minio.py` | CTAS ~30 gold datasets to `s3a://nyc-gold/`; defines `GOLD_DATASETS` |
| `scripts/materialize_to_postgres.py` | Copy gold tables to Postgres analytics; imports `GOLD_DATASETS` |
| `scripts/superset_bootstrap.py` | Superset DB + datasets + charts + dashboard (rebuilds `position_json`) |
| `scripts/superset_saved_queries.py` | Registers 24 saved SQL Lab queries targeting Trino (idempotent) |
| `airflow/dags/nyc_e2e_pipeline.py` | Monthly: spark → trino → dbt → gold+materialize → superset → analytics |
| `airflow/dags/nyc_analytics_refresh.py` | Weekly: dbt → gold+materialize → superset → analytics |
| `airflow/dags/nyc_cdc_pipeline.py` | Manual CDC: cdc_seed → cdc_register → cdc_bridge |
| `plan.md` | Pipeline issues & fix plan — 4 items ranked by severity |
| `plan_gold_layer.md` | Gold layer BI design — 20 dbt models across 3 stakeholders (Marketing/Sales/CEO) |
| `plan_data_quality_audit.md` | Full audit of all 75 Trino+Postgres tables |
| `plan_enhancement.md` | Pipeline enhancement plan (SCD, anomaly, incremental) |
| `check.md` | Quick reference: UI URLs, credentials, current row counts, bucket sizes |
| `AGENTS.md` | This file |
| `k8s/README.md` | Deprecation notice: why `k8s/` is stale vs Helm + list of known breakage |

## Runtime/Tooling Preferences

- **Python 3.11** inside containers (tools image)
- **Spark 3.5.1** (`apache/spark:3.5.1`)
- **Trino 435**
- **dbt-trino 1.11.x**
- **Superset 4.0.0** (custom image `nyc-superset` with `psycopg2-binary` + `trino` drivers)
- **Debezium 2.5**
- **Airflow 2.10.5**
- **PostgreSQL 16** (analytics) / **PostgreSQL 16** (CDC, logical replication enabled)
- **MinIO** S3-compatible (no specific version pinned)
- Package manager: pip (inside containers), no virtual env on host
- Skaffold v4beta3 with Helm deployer
- kind (k8s in Docker) with 3 nodes

## Testing & QA

### dbt Tests (`dbt build`)

- 30 models (16 staging/marts + 14 gold BI), 24 data tests, all `materialized=view` in dev.
- Generic tests: `not_null` on key columns in `dbt/tests/*.yml`.
- Singular tests: `payment_type_range`, `passenger_count_range`, `trip_distance_positive`, `total_not_less_than_fare`, `assert_minimum_rows`, `assert_recent_data`.
- **54/54 PASS** expected on `dbt build` (30 models + 24 tests).

### Analytics Validation

- 10 SQL questions in `sql/analytics_questions.sql` (run by `scripts/run_analytics_questions.py` in the `analytics_check` DAG task).
- Expected: 10/10 PASS.

### Mart Verification

- `scripts/verify_mart.py` — row counts for 4 mart tables.
- Expected: `dim_zone` = 261, `fact_trips` = ~8-10M, `mart_hourly_summary` = ~11K+, `mart_revenue_by_day` = ~90.

### Full Pipeline

- `nyc_e2e_pipeline` DAG: 10 tasks (spark_batch, cdc_seed, cdc_register, cdc_bridge, spark_streaming, trino_bootstrap, dbt_build, gold_export, materialize_postgres, superset_bootstrap, superset_saved_queries, analytics_check, anomaly_check).
- `nyc_analytics_refresh` DAG: 7 tasks (dbt_build, gold_export, materialize_postgres, superset_bootstrap, superset_saved_queries, analytics_check, anomaly_check).
- `nyc_cdc_pipeline` DAG: 3 tasks (cdc_seed, cdc_register, cdc_bridge).
- Trigger via Airflow UI or `airflow dags trigger` CLI.

### Skaffold Pre-deploy Hook (minimal sync)

The `skaffold.yaml` pre-deploy hook is intentionally minimal — no namespace clean, no data sync:
1. `mkdir -p /mnt/nyc-project /mnt/nyc-data` on kind-worker
2. `tar` sync project files (airflow/dags/, jobs/, scripts/, dbt/, charts/) to PVC
3. Load `:latest` custom images to all 3 kind nodes (docker save → tee → ctr import)

For full reset: `bash scripts/reset_ns.sh`

### Trino Configuration

- Container limit: 10Gi, JVM `-Xmx6G` with G1GC
- Query memory: 4GB per-node, 6GB cluster max, max 1 concurrent query
- Hive connector: file-based metastore at `/opt/project/data/trino-metastore`
- ⚠️ Known issue: **Trino OOMKilled** if too many CTAS queries run sequentially (gold_export with 40+ tables). Mitigation: reduced concurrency, increased memory. If OOM persists, split gold_export into batches or increase node resources.

### MinIO

- Credentials: `minio/minio123`
- API: port 9000 (S3), Console: port 9001 (web UI)
- ⚠️ Do NOT set `MINIO_BROWSER_REDIRECT_URL` — causes redirect loop on localhost
