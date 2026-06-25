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

## Data Quality Monitoring System (Design)

### Problem Statement

Pipeline has 13 nodes across 6 layers. Currently **11/13 nodes have no output monitoring** — data can be silently corrupted at any layer and propagate through the entire pipeline without detection until a human looks at a Superset chart.

```
LAYER                    NODE                   OUTPUT MONITORED?
─────────────────────────────────────────────────────────────────
Source                   cdc_seed               ❌ Mù
                         cdc_register           ❌ Mù
                         cdc_bridge             ❌ Mù
                         spark_batch            ❌ Mù
                         spark_streaming         ❌ Mù
Catalog                  trino_bootstrap        ❌ Mù
Transform                dbt_build              ✅ Một phần (not_null tests only)
Export                   gold_export            ❌ Mù
                         materialize_postgres   ❌ Mù
Presentation             superset_bootstrap     ❌ Mù
                         superset_saved_queries ❌ Mù
Verification             analytics_check        ✅ Có check, nhưng không có quyền block
                         anomaly_check          ✅ Có detect, nhưng không gửi alert
```

### Monitoring Architecture

Three complementary layers — Quality Gates (inline), Monitor DAG (out-of-band), Output Contracts (source of truth):

```
┌──────────────────────────────────────────────────────────────────────┐
│                     QUALITY GATE LAYER (inline, blocks pipeline)      │
│                                                                      │
│  spark_batch ──► verify_silver ──► trino_bootstrap ──► dbt_build     │
│  spark_stream ──┘                    │                                │
│                                      ├──► gold_export ──► verify_gold│
│                                      └──► materialize ──► verify_pg  │
│                                                              │        │
│                                              superset_bootstrap      │
│                                                     │                │
│                                              verify_superset         │
│                                                     │                │
│                                              verify_freshness        │
│                                                     │                │
│                                              analytics_check         │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│              MONITOR DAG (out-of-band, @hourly, read-only)            │
│                                                                      │
│  DAG: nyc_pipeline_monitor                                           │
│  ├── check_silver_health      → Trino: row count, null ratio         │
│  ├── check_gold_health        → Trino: 30 tables exist, row count    │
│  ├── check_postgres_health    → Postgres: row count matches gold     │
│  ├── check_superset_health    → Superset API: chart renders OK       │
│  ├── check_kafka_health       → Kafka: consumer group lag            │
│  ├── check_minio_health       → MinIO: bucket size, object count     │
│  └── report_health            → Aggregate → Slack/email if unhealthy │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│            OUTPUT CONTRACTS (YAML, source of truth per node)          │
│                                                                      │
│  contracts/silver.yaml     → spark_batch/spark_streaming output      │
│  contracts/gold.yaml       → gold_export output (30 datasets)        │
│  contracts/postgres.yaml   → materialize_postgres output             │
│  contracts/superset.yaml   → superset_bootstrap output               │
│  contracts/freshness.yaml  → global freshness SLA                    │
└──────────────────────────────────────────────────────────────────────┘
```

### Quality Gate Design (per node)

Each gate is a self-contained verify task added to the existing DAG. It runs independently, queries the output of the upstream node, and **blocks downstream tasks on failure** (exit code 1).

| Gate Task | Checks | Data Source | Blocks On Failure |
|---|---|---|---|
| `verify_silver` | Row count > 0, null ratio = 0 on required columns, AVG(trip_distance) in [1,20], MAX(pickup_date) freshness | Trino `hive.nyc.trips` | `trino_bootstrap` + all downstream |
| `verify_gold` | All 30 tables exist, each has row count > 0, row count matches dbt source view | Trino `hive.nyc_gold.*` | None currently (gold has no downstream, but prevents silent failure) |
| `verify_postgres` | Each table row count matches Trino gold counterpart, no zero-row tables | Postgres `nyc_analytics.public.*` vs Trino | `superset_bootstrap` |
| `verify_superset` | Datasets load successfully, dashboard charts render (no "No data"), chart metrics consistent with Trino | Superset API + Trino | `superset_saved_queries` |
| `verify_freshness` | `MAX(pickup_date)` ≤ 35 days stale, row count within 50% of 7-day average | Trino `hive.mart.fact_trips` | `analytics_check` |

### Gate Exit Semantics

```
PASS (exit 0)  → downstream runs normally
FAIL (exit 1)  → DAG task fails → Airflow retry 3x → if still fails → DAG marked failed + alert
```

Existing `analytics_check` and `anomaly_check` remain informational (exit 0 regardless) — they report, they don't block. Gates are the enforcement layer.

### Monitor DAG Design

Purpose: Continuous health monitoring **outside** pipeline execution. Catches degradation between scheduled pipeline runs (e.g., data growing too large for Trino, S3 bucket reaching capacity, Kafka consumer lag accumulating).

- **Schedule:** `@hourly` (lightweight queries only)
- **Access:** Read-only (no writes, no DDL)
- **Alert channel:** Slack webhook + email for CRITICAL, Slack only for WARNING
- **Idempotent:** Safe to run concurrently with main pipeline

```python
# Conceptual alert rules (not code)
ALERTS = {
    "silver_empty": {"severity": "CRITICAL", "condition": "row_count == 0"},
    "silver_stale": {"severity": "CRITICAL", "condition": "max_date < now - 35d"},
    "gold_missing_tables": {"severity": "CRITICAL", "condition": "any gold table row_count == 0"},
    "postgres_drift": {"severity": "WARNING", "condition": "pg_row_count != gold_row_count"},
    "superset_no_data": {"severity": "WARNING", "condition": "any chart returns 0 rows"},
    "kafka_lag": {"severity": "WARNING", "condition": "consumer_lag > 1000"},
    "minio_bucket_size_spike": {"severity": "WARNING", "condition": "bucket_size > 2x weekly_avg"},
    "trino_memory": {"severity": "CRITICAL", "condition": "pod_restart_count > 3 in 1h"},
    "airflow_scheduler_down": {"severity": "CRITICAL", "condition": "health_check fails 3x consecutive"},
}
```

### Cross-Node Reconciliation

Beyond individual gates, pipeline needs **pairwise reconciliation** between adjacent nodes:

```
row_count(spark_input) == row_count(silver) + row_count(quarantine)
row_count(silver)       == row_count(mart.fact_trips)
row_count(mart.*)       == row_count(gold_export.*)
row_count(gold.*)       == row_count(postgres.*)
```

Each reconciliation is a separate verify task. If any pair mismatches → pipeline blocked at that boundary.

### Failure Mode Coverage

| Failure Scenario | Detected By | Time To Detect |
|---|---|---|
| MinIO bucket wrong name → Spark reads 0 rows | `verify_silver`: row_count == 0 → block | < 2 min after Spark completes |
| Spark type cast bug → fare_amount all null | `verify_silver`: null ratio > 0 → block | < 2 min |
| Spark crash + append → duplicate rows | `verify_silver`: row_count > expected → block | < 2 min |
| Trino OOM → gold_export partial fail | `verify_gold`: missing tables → block | < 2 min |
| materialize fail → Postgres empty table | `verify_postgres`: row_count mismatch → block | < 2 min |
| Superset cache stale → shows old data | `verify_superset`: chart SQL vs Trino mismatch | < 2 min |
| NYC TLC stops publishing → no new data | `verify_freshness`: max_date stale > 35d → block | < 2 min |
| Kafka consumer lag → CDC data delayed | Monitor DAG `check_kafka_health` | < 1 hour |
| S3 bucket approaching capacity | Monitor DAG `check_minio_health` | < 1 hour |

### Implementation Path

```
Phase 1 (Tuần 1):   verify_silver + verify_freshness → chặn lỗi Spark và data stale
Phase 2 (Tuần 2):   verify_postgres + verify_gold → chặn lỗi materialize và gold export
Phase 3 (Tuần 3):   verify_superset + cross-node reconciliation → chặn lỗi UI và drift
Phase 4 (Tuần 4):   Monitor DAG + alert pipeline (Slack/Email) → phát hiện ngoài pipeline run
Phase 5 (Tuần 5+):  Output contracts YAML → source of truth, dễ review, dễ mở rộng
```

### Design Principles

1. **Gates block, monitors report.** Gates run inline in the main DAG and stop downstream on failure. Monitor DAG runs out-of-band and alerts without blocking.
2. **Each node owns its output contract.** The Spark team owns `contracts/silver.yaml`. The dbt team owns `contracts/gold.yaml`. No centralized gatekeeper bottleneck.
3. **Reconciliation over validation.** Row count reconciliation catches more bugs than schema validation. Do both, but reconciliation first.
4. **Alert on absence, not just error.** Missing data is as bad as corrupt data. Freshness checks are mandatory.
5. **Contracts before code.** Write the YAML contract first (what "good" looks like), then implement the check. The contract is reviewable by business stakeholders, not just engineers.

### Pipeline Health Dashboard (Design)

A single Superset dashboard — the "Pipeline Control Room" — displays real-time health for every node in the pipeline. Each node gets its own row with status indicator (🟢🟡🔴), the metric being checked, the current value, the SLO threshold, and when it was last verified.

#### Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│  PIPELINE HEALTH — NYC Taxi E2E                            [12:34]  │
│  Last DAG run: 2026-06-23 10:26  Status: FAILED                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─ INGEST ─────────────────────────────────────────────────────┐  │
│  │ 🟢 cdc_seed          rows=1000          PASS         23/06   │  │
│  │ 🟢 cdc_register      connector=RUNNING  PASS         23/06   │  │
│  │ 🟢 cdc_bridge        events=445/s       PASS         23/06   │  │
│  │ 🟢 spark_batch       rows=8,480,375     PASS   ✓ 10M 23/06   │  │
│  │ 🔴 spark_streaming   rows=0             FAIL   ✗ >0  23/06   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌─ CATALOG ────────────────────────────────────────────────────┐  │
│  │ 🟢 trino_bootstrap   tables=3           PASS         23/06   │  │
│  │ 🟢 trino_uptime      uptime=99.8%       PASS   ≥99%   live   │  │
│  │ 🟢 minio_health      size=454MB         PASS         live   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌─ TRANSFORM ──────────────────────────────────────────────────┐  │
│  │ 🟢 dbt_build         models=30/30       PASS         23/06   │  │
│  │ 🟢 dbt_tests         pass=54/54         PASS   54/54 23/06   │  │
│  │ 🟢 dbt_duration      time=3m12s         PASS   ≤5m   23/06   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌─ EXPORT ─────────────────────────────────────────────────────┐  │
│  │ 🔴 gold_export       ok=0/30            FAIL   30/30 23/06   │  │
│  │ 🔴 materialize       postgres rows=0    FAIL   ≠gold 23/06   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌─ PRESENTATION ───────────────────────────────────────────────┐  │
│  │ 🔴 superset_health   charts=3/7         FAIL   7/7   23/06   │  │
│  │ 🟠 superset_cache    staleness=12h      WARN   ≤1h   23/06   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌─ END-TO-END ─────────────────────────────────────────────────┐  │
│  │ 🟢 freshness        max_date=2024-06-15 PASS   ≤35d  23/06   │  │
│  │ 🟢 reconciliation   raw→silver=OK      PASS   ±1%   23/06   │  │
│  │ 🟢 reconciliation   silver→gold=OK     PASS   ±1%   23/06   │  │
│  │ 🔴 reconciliation   gold→postgres=FAIL FAIL   0%    23/06   │  │
│  │ 🟢 anomaly_check     anomalies=0        PASS   ≤5%   23/06   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  OVERALL: 🔴 4 FAILING  🟠 1 WARNING  🟢 14 PASSING               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### Status Indicator Logic

| Indicator | Meaning | Trigger | Action |
|---|---|---|---|
| 🟢 PASS | Metric trong ngưỡng | Giá trị hiện tại thỏa mãn SLO | Không |
| 🟠 WARN | Metric gần ngưỡng | 70%-99% của ngưỡng (ví dụ: freshness 30d/35d) | Slack |
| 🔴 FAIL | Metric vượt ngưỡng | Giá trị hiện tại vi phạm SLO | Slack + Email + Block downstream |
| ⬛ STALE | Chưa được verify | Last check > 2x schedule interval | Slack |

#### Data Sources for Each Row

Each row in the dashboard pulls from a different source — no single point of failure for the dashboard itself:

```
┌──────────────────────────────────────────────────────────────────┐
│                     DASHBOARD DATA SOURCES                        │
│                                                                   │
│  spark_batch status    → Airflow API  (task_instance state)      │
│  spark row count       → Trino       (hive.nyc.trips)            │
│  dbt test pass rate    → dbt         (run_results.json)          │
│  gold export status    → Airflow API + Trino (hive.nyc_gold.*)   │
│  postgres row count    → Postgres    (nyc_analytics.public.*)    │
│  superset health       → Superset API (charts render check)      │
│  minio health          → MinIO API   (/minio/health/live)        │
│  kafka health          → Kafka API   (consumer group lag)        │
│  freshness             → Trino       (MAX(pickup_date))          │
│  reconciliation        → Trino + Postgres (row count diff)       │
└──────────────────────────────────────────────────────────────────┘
```

#### How It Refreshes

Dashboard data is populated by a lightweight script (`scripts/generate_health_report.py`) that runs every 5 minutes via the Monitor DAG. The script:

1. Queries every data source in parallel (Airflow API, Trino, Postgres, MinIO, Kafka, Superset)
2. Compares each metric to its SLO threshold
3. Writes a JSON health report to a Postgres table `pipeline_health.checks`
4. Superset dashboard queries this table directly (single source, fast refresh)

```
┌──────────┐    ┌─────────────────┐    ┌──────────────┐    ┌──────────┐
│ Sources  │    │ health_report.py │    │  Postgres    │    │ Superset │
│ Airflow   │───►│ (runs @5min)     │───►│ pipeline_    │───►│ Dashboard│
│ Trino     │    │ compare→PASS/FAIL│    │ health.checks│    │ renders  │
│ Postgres  │    │                  │    │              │    │ 🟢🟠🔴   │
│ MinIO     │    └─────────────────┘    └──────────────┘    └──────────┘
│ Kafka     │
│ Superset  │
└──────────┘
```

#### Health Report Table Schema

```sql
-- Postgres table: pipeline_health.checks
-- One row per check, upserted every 5 minutes
CREATE TABLE pipeline_health.checks (
    check_id        TEXT PRIMARY KEY,       -- e.g. 'spark_batch.row_count'
    layer           TEXT,                   -- 'ingest', 'catalog', 'transform', 'export', 'presentation', 'e2e'
    node            TEXT,                   -- 'spark_batch', 'dbt_build', 'gold_export', ...
    metric          TEXT,                   -- 'row_count', 'status', 'freshness', ...
    current_value   TEXT,                   -- '8,480,375'
    expected_slo    TEXT,                   -- '> 0'
    status          TEXT,                   -- 'PASS', 'WARN', 'FAIL', 'STALE'
    checked_at      TIMESTAMPTZ,            -- last verification time
    error_message   TEXT                    -- NULL if PASS, error detail if FAIL
);
```

#### Per-Node SLO Reference

| Node | Metric | SLO | Criticality |
|---|---|---|---|
| `cdc_seed` | rows_inserted | = 1000 | 🟢 Low |
| `cdc_register` | connector_status | = 'RUNNING' | 🟢 Low |
| `cdc_bridge` | events_per_second | > 0 | 🟢 Low |
| `spark_batch` | silver_row_count | > 1,000,000 | 🔴 Critical |
| `spark_batch` | quarantine_ratio | ≤ 15% | 🟠 Warning |
| `spark_batch` | null_ratio_fare_amount | = 0% | 🔴 Critical |
| `spark_streaming` | silver_row_count | > 0 | 🟠 Warning |
| `trino_bootstrap` | tables_registered | = 3 | 🔴 Critical |
| `trino_uptime` | pod_ready | = true | 🔴 Critical |
| `minio_health` | health_endpoint | = 200 | 🔴 Critical |
| `minio_health` | bucket_size_bytes | < 10GB | 🟠 Warning |
| `dbt_build` | models_built | = 30 | 🔴 Critical |
| `dbt_build` | tests_passed | = 54 | 🔴 Critical |
| `dbt_build` | duration_seconds | ≤ 300 | 🟠 Warning |
| `gold_export` | tables_ok | = 30 | 🔴 Critical |
| `gold_export` | total_rows | matches mart.* | 🔴 Critical |
| `materialize_postgres` | pg_row_count | = gold_row_count (all tables) | 🔴 Critical |
| `superset_bootstrap` | datasets_ready | = 7 | 🟠 Warning |
| `superset_health` | charts_rendering | = 7/7 | 🟠 Warning |
| `superset_cache` | cache_age_minutes | ≤ 60 | 🟠 Warning |
| `freshness` | max_pickup_date_age_days | ≤ 35 | 🔴 Critical |
| `reconciliation_01` | raw_rows − (silver + quarantine) | = 0 | 🔴 Critical |
| `reconciliation_02` | silver − mart.fact_trips | = 0 | 🔴 Critical |
| `reconciliation_03` | mart.* − gold.* | = 0 | 🔴 Critical |
| `reconciliation_04` | gold.* − postgres.* | = 0 | 🔴 Critical |
| `anomaly_check` | anomaly_days_ratio | ≤ 5% | 🟠 Warning |
| `kafka_health` | consumer_lag | < 1000 | 🟠 Warning |
| `airflow_scheduler` | scheduler_healthy | = true | 🔴 Critical |

### Pipeline Health SLOs

Seven pillars define whether the pipeline is "healthy". All SLOs are checked by the Monitor DAG and displayed on the Pipeline Health Dashboard.

| Pillar | Key Metric | SLO Threshold | Criticality |
|---|---|---|---|
| **Freshness** | `MAX(pickup_date)` staleness | ≤ 35 days | 🔴 |
| | Raw source availability | New partition within 45 days | 🔴 |
| **Completeness** | Silver row count | > 0 AND within ±20% of prior run | 🔴 |
| | Gold table count | 30/30 tables exported | 🔴 |
| | Postgres ↔ Gold row count | 0% mismatch | 🔴 |
| **Correctness** | dbt test pass rate | 54/54 (100%) | 🔴 |
| | Null ratio on required columns | 0% | 🔴 |
| | Distribution sanity vs 3-month avg | ≤ 50% variance | 🟠 |
| | Business assertion (revenue sum) | ≤ 1% mismatch | 🔴 |
| **Volume** | Daily trip count | 50%–200% of 7-day rolling avg | 🟠 |
| | Silver storage growth | ≤ 500MB/month | 🟠 |
| **Latency** | Total DAG duration | ≤ 45 minutes | 🟠 |
| | Spark batch duration | ≤ 20 minutes | 🟠 |
| | gold_export duration | ≤ 10 minutes | 🟠 |
| **Availability** | DAG success rate (30-day) | ≥ 95% | 🔴 |
| | Trino uptime | ≥ 99% | 🔴 |
| | MinIO uptime | ≥ 99% | 🔴 |
| **Schema** | Column count hive.nyc.trips | = 25 | 🟠 |
| | Partition count | Increases +1 per month | 🟠 |

When any 🔴 SLO fails → pipeline blocked + immediate Slack + email. When any 🟠 SLO fails → Slack notification only.

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
