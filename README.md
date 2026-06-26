# NYC Taxi Data Pipeline

> **Languages**: [English](README.md) (this file) · [Tiếng Việt](README_vi.md)
>
> End-to-end data engineering platform: batch + streaming + CDC ingestion from
> NYC TLC Yellow Taxi records (Parquet) and PostgreSQL CDC, transformed through
> Spark → Trino → dbt → Superset, orchestrated by Airflow on Kubernetes (kind
> + Skaffold + Helm).

---
## 1. What this project does

The pipeline ingests NYC Yellow Taxi trip records from two sources, validates
and enriches them, and exposes the results to three audiences:

| Audience | Output | How they use it |
|---|---|---|
| **Data engineers** | 33 gold Parquet datasets in `s3a://nyc-gold/` | Build new pipelines, train models, audit, export |
| **BI / marketing / sales / CEO** | 30 Superset charts + dashboard on Postgres Analytics | Operational and strategic decisions |
| **Data engineers / SRE** | Trino views, dbt marts, Airflow DAGs | Ad-hoc SQL, dbt lineage, pipeline debugging |

### Two ingestion paths

- **Batch** (`jobs/spark_local_batch.py`) — backfills historical months from
  Parquet in `s3a://nyc-raw/yellow_taxi/year=YYYY/month=MM/`. Supports
  `--incremental` to process only partitions newer than what is already in
  silver.
- **Streaming** (`jobs/spark_stream_taxi_events.py`) — consumes
  `taxi.trip.events` from Kafka via `foreachBatch`, applies the same
  enrichment + validation as the batch job, appends to the same silver path.

### Optional CDC path

`Postgres 16 (WAL logical)` → `Debezium 2.5` → Kafka topic
`nyc_cdc.public.trips` → `scripts/cdc_bridge.py` → Kafka topic
`taxi.trip.events` → Spark Streaming → silver. Three standalone DAG tasks
(`cdc_seed` → `cdc_register` → `cdc_bridge`) seed the Postgres table from a
Parquet file, register the Debezium connector, and bridge CDC events to the
standard taxi event format.

### Outputs

- **Valid trips** → `s3a://nyc-silver/trips/` partitioned by
  `pickup_year/pickup_month` (current dataset: ~8.4M rows from Jan–Mar 2024).
- **Invalid trips** → `s3a://nyc-quarantine/invalid_trips/` (unpartitioned,
  with `validation_errors ARRAY<VARCHAR>` and `quarantine_ts`).
- **dbt views** → `hive.nyc.*` (raw external), `hive.mart.*` (dbt marts),
  exposed to Trino via the Hive connector.
- **Gold Parquet** → `s3a://nyc-gold/<dataset>/` — 33 datasets from
  `scripts/export_gold_to_minio.py` (CTAS).
- **Postgres analytics** → `nyc_analytics.public.*` — atomic swap from
  `scripts/materialize_to_postgres.py`.
- **Superset** → 30 chart definitions + dashboard on `nyc_analytics.public.*`.

---

## 2. Architecture

### 2.1 Component diagram

```mermaid
flowchart TD
    subgraph SOURCE["1. Source"]
        RAW["📦 MinIO nyc-raw<br/>Parquet"]
        PGC["🐘 Postgres CDC<br/>nyc_taxi.trips"]
    end

    subgraph INGEST["2. Ingest (Spark)"]
        SB["⚡ spark_batch<br/>local[*]"]
        SS["⚡ spark_streaming<br/>Kafka foreachBatch"]
    end

    subgraph CDC["3. CDC chain (optional)"]
        DZ["🔗 Debezium 2.5"]
        KFK["📨 Kafka<br/>taxi.trip.events"]
        BR["🌉 cdc_bridge"]
    end

    subgraph STORAGE["4. MinIO S3"]
        SIL["✅ nyc-silver/trips<br/>partitioned"]
        QUA["⚠️ nyc-quarantine"]
        LKP["📋 nyc-lookup"]
        GLD["📦 nyc-gold/*<br/>33 datasets"]
    end

    subgraph CATALOG["5. Catalog (Trino 435)"]
        TR["Trino · Hive connector<br/>hive.nyc · hive.mart"]
    end

    subgraph TRANSFORM["6. Transform (dbt-trino)"]
        STG["3 staging views"]
        MARTS["8 mart views"]
        GOLD["19 gold views"]
    end

    subgraph SERVE["7. Serve"]
        MAT["📊 Postgres analytics<br/>nyc_analytics.public.*"]
        SUP["📈 Superset<br/>30 charts"]
    end

    RAW --> SB
    LKP --> SB
    LKP --> SS
    SB -->|valid| SIL
    SB -->|invalid| QUA
    SS -->|valid| SIL
    SS -->|invalid| QUA

    PGC --> DZ --> KFK --> BR --> KFK
    KFK --> SS

    SIL --> TR
    QUA --> TR
    LKP --> TR
    TR --> STG --> MARTS --> GOLD
    GOLD --> GLD
    GOLD --> MAT
    MAT --> SUP
    TR --> SUP
```

### 2.2 Tech stack

| Layer | Technology | Version |
|---|---|---|
| Storage | MinIO S3 | (latest) |
| Batch processing | Apache Spark | 3.5.1 |
| Stream processing | Apache Spark Structured Streaming | 3.5.1 |
| Messaging | Confluent Kafka + ZooKeeper | 7.6.1 |
| CDC | Debezium Kafka Connect | 2.5 |
| SQL catalog | Trino (Hive connector, file-based metastore) | 435 |
| Transform | dbt-trino | ≥1.7, <2.0 |
| Visualization | Apache Superset | 4.0.0 |
| Orchestration | Apache Airflow (KubernetesPodOperator) | 2.10.5 |
| Analytics store | PostgreSQL 16 | 16-alpine |
| Container / deploy | kind + Skaffold + Helm | kind ≥0.20, Skaffold v2, Helm ≥3 |
| Alt: local dev | Docker Compose | (16 services) |

### 2.3 Data flow at a glance

```mermaid
flowchart TD
    subgraph SRC["Sources"]
        RAW["📦 MinIO nyc-raw<br/>Parquet"]
        KAFKA[("📨 Kafka<br/>taxi.trip.events")]
    end

    subgraph CDC["CDC chain (optional)"]
        PG["🐘 Postgres 16<br/>WAL logical"]
        DZ["🔗 Debezium 2.5"]
        BR["🌉 cdc_bridge"]
    end

    subgraph INGEST["Ingest (Spark)"]
        SB["⚡ spark_batch<br/>local[*]"]
        SS["⚡ spark_streaming<br/>Kafka foreachBatch"]
    end

    subgraph STORAGE["MinIO S3"]
        SIL["✅ nyc-silver/trips<br/>(partitioned)"]
        QUA["⚠️ nyc-quarantine"]
    end

    subgraph CATALOG["Trino 435"]
        TR["Trino · hive.nyc.trips<br/>Trino · hive.nyc.invalid_trips"]
    end

    subgraph TRANSFORM["dbt-trino"]
        STG["3 staging views"]
        MRT["8 mart views"]
        GLD["19 gold views"]
    end

    subgraph EXPORT["Export"]
        GE["gold_export<br/>CTAS → s3://nyc-gold/"]
        MP["materialize_to_postgres<br/>atomic swap"]
    end

    subgraph SERVE["Serve"]
        PGDB[("Postgres<br/>nyc_analytics.public")]
        SUP["📈 Superset<br/>30 charts + dashboard"]
    end

    RAW --> SB
    KAFKA --> SS
    PG --> DZ --> KAFKA
    KAFKA --> BR --> KAFKA

    SB --> SIL
    SB --> QUA
    SS --> SIL
    SS --> QUA

    SIL --> TR
    QUA --> TR
    TR --> STG --> MRT --> GLD
    GLD --> GE
    GLD --> MP
    GE --> SUP
    MP --> PGDB --> SUP
```

---

## 3. Repository layout

```mermaid
flowchart LR
    subgraph AIRFLOW["airflow/dags/ — 3 DAGs"]
        AF1["nyc_e2e_pipeline.py<br/>13 tasks, manual"]
        AF2["nyc_analytics_refresh.py<br/>7 tasks, @weekly"]
        AF3["nyc_cdc_pipeline.py<br/>3 tasks, manual"]
    end

    subgraph JOBS["jobs/ — 5 Spark processors"]
        J1["spark_local_batch.py<br/>read raw → enrich+validate<br/>→ silver/quarantine"]
        J2["spark_stream_taxi_events.py<br/>Kafka → enrich+validate<br/>→ silver/quarantine"]
        J3["spark_quality_report.py<br/>PyArrow, no Spark runtime"]
        J4["spark_batch_backfill.py<br/>legacy placeholder"]
        J5["kafka_stream_processor.py<br/>Python-only, alternative"]
    end

    subgraph SCRIPTS["scripts/ — 30 utility scripts"]
        S1["trino_register.py<br/>trino_sync_partitions.py<br/>export_gold_to_minio.py<br/>materialize_to_postgres.py"]
        S2["superset_bootstrap.py<br/>superset_saved_queries.py<br/>superset_check.py"]
        S3["run_analytics_questions.py<br/>check_anomaly.py<br/>verify_mart.py"]
        S4["cdc_seed.py<br/>cdc_register_connector.py<br/>cdc_bridge.py"]
        S5["create_kafka_topics.py<br/>start_streaming_job.sh<br/>run_dbt.sh"]
        S6["download_data.sh<br/>local_e2e_test.sh<br/>local_e2e_full_9_5m.sh"]
        S7["k8s_ui.sh<br/>cluster_up.sh<br/>setup_kind_images.sh<br/>reset_ns.sh"]
    end

    subgraph DBT["dbt/ — dbt-trino project"]
        D1["dbt_project.yml · profiles.yml<br/>(dev target → Trino)"]
        D2["models/staging/ — 3 models<br/>stg_trips · stg_zones · stg_invalid_trips"]
        D3["models/marts/ — 8 models<br/>fact_trips · dim_zone · …"]
        D4["models/gold/ — 19 BI models<br/>executive · customer · vendor · …"]
        D5["tests/ — 6 singular + 21 generic<br/>= 27 tests"]
    end

    subgraph DOCKER["docker/ — Dockerfiles + entrypoints"]
        DK1["tools.Dockerfile<br/>dbt.Dockerfile<br/>airflow.Dockerfile"]
        DK2["superset/ — config + entrypoint"]
        DK3["trino/ — hive.properties, jvm.config, …"]
        DK4["*.sh — 10 entrypoint scripts"]
    end

    subgraph ROOT["Root files"]
        R1["charts/nyc-taxi/ — Helm chart<br/>44 valid K8s manifests"]
        R2["docker-compose.yml — 16 services"]
        R3["skaffold.yaml — 4 build artifacts"]
        R4["kind.yaml.template — 3-node cluster"]
        R5["Makefile — 29 targets"]
        R6["AGENTS.md · docs/ · README.md"]
    end

    AIRFLOW --> DBT
    JOBS --> DBT
    SCRIPTS --> DBT
    DOCKER --> ROOT
```

---

## 4. Components in detail

```mermaid
flowchart LR
    subgraph SRC["Source"]
        RAW["📦 nyc-raw<br/>Parquet"]
        LKP["📋 nyc-lookup<br/>taxi_zone_lookup.csv"]
        PG["🐘 Postgres CDC<br/>(optional)"]
    end

    subgraph SPARK["Spark jobs/jobs/"]
        SB["⚡ spark_local_batch.py<br/>local[*] · --incremental"]
        SS["⚡ spark_stream_taxi_events.py<br/>Kafka foreachBatch"]
    end

    subgraph MINIO["MinIO S3"]
        SIL["✅ nyc-silver/trips<br/>partitioned"]
        QUA["⚠️ nyc-quarantine"]
    end

    subgraph TRINO["Trino scripts/"]
        TR["trino_register.py<br/>hive.nyc.trips<br/>hive.nyc.invalid_trips<br/>hive.nyc.taxi_zone_lookup"]
    end

    subgraph DBT["dbt/ — 30 models"]
        STG["staging/<br/>3 views"]
        MARTS["marts/<br/>8 views"]
        GOLD["gold/<br/>19 views"]
    end

    subgraph SCRIPTS["Export scripts/"]
        GE["export_gold_to_minio.py<br/>33 datasets CTAS<br/>→ s3://nyc-gold/"]
        MP["materialize_to_postgres.py<br/>atomic swap"]
    end

    subgraph SERVE["Serve"]
        PGDB[("Postgres<br/>nyc_analytics.public")]
        SUP["📈 Superset<br/>46 datasets + 30 charts"]
    end

    RAW --> SB
    LKP --> SB
    PG --> SS
    SS --> SIL
    SS --> QUA
    SB --> SIL
    SB --> QUA
    SIL --> TR
    QUA --> TR
    LKP --> TR
    TR --> STG --> MARTS --> GOLD
    GOLD --> GE
    GOLD --> MP
    GE --> SUP
    MP --> PGDB --> SUP
```

### 4.1 Spark batch — `jobs/spark_local_batch.py`

```mermaid
flowchart TD
    subgraph INPUTS["Inputs"]
        RAW["📦 s3a://nyc-raw<br/>yellow_taxi/year=*/month=*/*.parquet"]
        LKP["📋 s3a://nyc-lookup<br/>taxi_zone_lookup.csv<br/>(265 zones)"]
    end

    subgraph WAIT["Health probe"]
        HC{"MinIO<br/>/health/live<br/>120s timeout"}
    end

    subgraph INCR["Optional: --incremental"]
        MAX["Read max(pickup_year),<br/>max(pickup_month)<br/>from existing silver"]
        FILTER["Filter input to<br/>newer partitions only"]
        SKIP{"Anything<br/>new?"}
    end

    subgraph TRANSFORM["Transformations"]
        CAST["Cast types<br/>VendorID→int, timestamps, etc."]
        ZONE["Zone cleaning<br/>Unknown / N/A / NV → NULL"]
        ENRICH["Enrich columns<br/>trip_id (xxhash64)<br/>event_ts, ingestion_ts<br/>pickup_year/month/date/hour"]
        FILE_FILTER["input_file_name() filter<br/>drop edge rows from<br/>adjacent months"]
    end

    subgraph ZONE_JOIN["Zone lookup join"]
        PZ["pickup_zones<br/>(renamed columns)"]
        DZ["dropoff_zones<br/>(renamed columns)"]
        JOIN["left join × 2"]
    end

    subgraph VALIDATE["10 validation rules<br/>(see §4.5)"]
        ERR["error_array<br/>collect error tags"]
        CHECK["is_valid = size(errors) == 0"]
    end

    subgraph OUTPUT["Output (mode=append)"]
        SIL["✅ s3a://nyc-silver/trips<br/>partitioned by<br/>pickup_year/pickup_month"]
        QUA["⚠️ s3a://nyc-quarantine/invalid_trips<br/>+ validation_errors<br/>+ quarantine_ts"]
    end

    RAW --> HC
    LKP --> HC
    HC -->|ready| RAW
    HC -->|ready| LKP

    RAW --> INCR
    MAX --> FILTER
    FILTER --> SKIP
    SKIP -->|No| DONE["Early return<br/>no new data"]
    SKIP -->|Yes| CAST

    CAST --> ZONE
    ZONE --> ENRICH
    ENRICH --> FILE_FILTER
    FILE_FILTER --> JOIN

    LKP --> PZ
    LKP --> DZ
    PZ --> JOIN
    DZ --> JOIN

    JOIN --> ERR
    ERR --> CHECK
    CHECK -->|valid rows| SIL
    CHECK -->|invalid rows| QUA
```

- **Master**: `local[*]` (single pod, runs both driver and executor).
- **Input**: `s3a://nyc-raw/yellow_taxi/year=*/month=*/*.parquet`.
- **Lookups**: `s3a://nyc-lookup/taxi_zone_lookup.csv` (265 rows).
- **Output**: `s3a://nyc-silver/trips` (valid, partitioned `pickup_year/pickup_month`,
  `mode("append")`) and `s3a://nyc-quarantine/invalid_trips` (unpartitioned).
- **CLI args**: `--input`, `--lookup`, `--silver`, `--quarantine`, `--year`,
  `--month`, `--incremental`.
- **S3A** packages: `org.apache.hadoop:hadoop-aws:3.3.4` +
  `com.amazonaws:aws-java-sdk-bundle:1.12.262` via `--packages`.
- **MinIO health probe** before reading (`/minio/health/live`, 120s).
- **Incremental** mode: reads `max(pickup_year), max(pickup_month)` from the
  existing silver path, filters input to strictly newer partitions. Returns
  early if there is nothing new.
- **Year/month filter** via `input_file_name()` — drops edge rows that
  belong to adjacent months even when the file is named for the target month.
- **Zone cleaning**: `Borough.isin("Unknown","N/A","NV")` → `NULL` (handles
  NYC TLC zone IDs 264 and 265).
- **Enriched columns**: `trip_id` (xxhash64 of `pickup_ts|pickup_loc|dropoff_loc`),
  `event_ts`, `ingestion_ts`, `pickup_date`, `pickup_hour`, `pickup_year`,
  `pickup_month`, plus 6 zone fields × 2 (pickup + dropoff).
- **10 validation rules** (see §4.5).

### 4.2 Spark streaming — `jobs/spark_stream_taxi_events.py`

```mermaid
flowchart TD
    subgraph KAFKA["Kafka source"]
        T["📨 taxi.trip.events<br/>subscribe=topic<br/>startingOffsets=earliest<br/>failOnDataLoss=false"]
    end

    subgraph READ["ReadStream"]
        RAW["spark.readStream<br/>.format(kafka)"]
        PARSE["from_json(value, EVENT_SCHEMA)<br/>20 fields"]
    end

    subgraph TRANSFORM["Transformations"]
        TS["to_timestamp<br/>pickup_ts · dropoff_ts · event_ts"]
        IDS["ingestion_ts · pickup_date<br/>pickup_hour · pickup_year<br/>pickup_month"]
        TRIPID["trip_id<br/>xxhash64(pickup_ts|pickup_loc|dropoff_loc)"]
    end

    subgraph ZONE["Zone join"]
        PZ["pickup_zones<br/>(4 cols)"]
        DZ["dropoff_zones<br/>(4 cols)"]
        JOIN["left join × 2"]
    end

    subgraph VALIDATE["11 validation rules<br/>(10 from batch + event_id_null)"]
        ERR["error_array<br/>collect tags"]
        IS["is_valid = size(errors) == 0"]
    end

    subgraph FB["foreachBatch write loop"]
        PERSIST["batch_df.persist()"]
        SPLIT["filter is_valid == true / false"]
        WRITEV["valid → silver<br/>partitionBy(year, month)"]
        WRITEQ["invalid → quarantine<br/>(unpartitioned)"]
        UNPERSIST["batch_df.unpersist()"]
    end

    subgraph CKPT["Checkpoint + trigger"]
        CP["s3a://nyc-silver/checkpoints/<br/>spark_stream_taxi_events/"]
        TR["--trigger-available-now<br/>(one-shot)"]
    end

    KAFKA --> RAW
    RAW --> PARSE
    PARSE --> TS
    TS --> IDS
    IDS --> TRIPID
    TRIPID --> JOIN

    ZONE_JOIN_LKP["📋 s3a://nyc-lookup<br/>taxi_zone_lookup.csv"] --> PZ
    ZONE_JOIN_LKP --> DZ
    PZ --> JOIN
    DZ --> JOIN

    JOIN --> ERR
    ERR --> IS
    IS --> PERSIST
    PERSIST --> SPLIT
    SPLIT --> WRITEV
    SPLIT --> WRITEQ
    WRITEV --> UNPERSIST
    WRITEQ --> UNPERSIST

    CKPT -.-> PERSIST
    TR -.-> PERSIST
```

- **Master**: `local[*]` (one-shot via `--trigger-available-now`, otherwise
  continuous).
- **Kafka**: `subscribe=taxi.trip.events`,
  `startingOffsets=earliest`, `failOnDataLoss=false`.
- **Schema**: 20 fields declared in `EVENT_SCHEMA` (vendor_id, pickup_datetime,
  dropoff_datetime, passenger_count, trip_distance, payment_type, …).
- **Output paths**: same as batch (`s3a://nyc-silver/trips` and
  `s3a://nyc-quarantine/invalid_trips`).
- **Checkpoint**: `s3a://nyc-silver/checkpoints/spark_stream_taxi_events/taxi.trip.events`.
- **`foreachBatch`**: persists the batch DataFrame, splits valid/invalid,
  writes each, then unpersists.
- **11 validation rules**: 10 from batch + `event_id_null` (streaming-only).
- **CLI args**: `--bootstrap-server`, `--topic`, `--lookup-path`,
  `--silver-path`, `--quarantine-path`, `--checkpoint-path`,
  `--trigger-available-now`.

### 4.3 CDC scripts

```mermaid
flowchart LR
    subgraph SEED["cdc_seed.py — one-time seed"]
        P_IN["📦 Parquet file<br/>--input"]
        PANDAS["Pandas read<br/>+ column rename<br/>+ dropna + sample(max_rows)"]
        TRUNC["TRUNCATE trips<br/>RESTART IDENTITY CASCADE"]
        INSERT["SQLAlchemy to_sql<br/>method='multi'"]
        PG_OUT[("🐘 Postgres<br/>nyc_taxi.trips")]
        P_IN --> PANDAS --> TRUNC --> INSERT --> PG_OUT
    end

    subgraph REG["cdc_register_connector.py — idempotent"]
        WAIT_DZ["wait Debezium REST<br/>/connectors (60s timeout)"]
        DEL["DELETE if exists<br/>(idempotency)"]
        POST["POST /connectors<br/>name: nyc-postgres-connector"]
        CFG["Config:<br/>plugin.name=pgoutput<br/>transforms=unwrap<br/>ExtractNewRecordState<br/>snapshot.mode=never<br/>poll.interval.ms=500"]
        DZ_SVC[("🔗 Debezium<br/>svc-debezium:8083")]
        WAIT_DZ --> DEL --> POST --> DZ_SVC
        CFG -.-> POST
    end

    subgraph BRG["cdc_bridge.py — CDC → standard format"]
        CONS["KafkaConsumer<br/>input-topic: nyc_cdc.public.trips<br/>group_id: cdc-bridge-{uuid}<br/>enable_auto_commit=false"]
        TRANS["transform(event)<br/>unwrap → flat event<br/>add event_id, source_file, etc."]
        PROD["KafkaProducer<br/>output-topic: taxi.trip.events<br/>linger_ms=100, batch_size=64KB<br/>compression=gzip<br/>flush every 500 events"]
        KFK[("📨 Kafka<br/>taxi.trip.events")]
        CONS --> TRANS --> PROD --> KFK
    end

    PG_OUT -->|WAL logical| DZ_SVC
    DZ_SVC -->|Debezium envelope| CONS
```

- `scripts/cdc_seed.py` — reads a Parquet file with Pandas, renames columns,
  TRUNCATEs `nyc_taxi.trips`, inserts via `to_sql(method='multi')`. Default
  `--max-rows 5000`, sampling `random_state=42`.
- `scripts/cdc_register_connector.py` — POSTs the Debezium connector config
  to `${debezium-url}/connectors`. Idempotent (DELETE then POST). Key settings:
  `plugin.name=pgoutput`, `transforms=unwrap` with
  `ExtractNewRecordState`, `snapshot.mode=never`,
  `transforms.unwrap.drop.tombstones=false`, `poll.interval.ms=500`.
- `scripts/cdc_bridge.py` — consumes the Debezium topic, transforms each
  event to the standard NYC Taxi format (same fields as the streaming schema),
  produces to `taxi.trip.events`. Performance: `linger_ms=100`,
  `batch_size=65536`, `compression_type=gzip`. Async send with periodic
  flush every `--flush-interval` events. **Group id is generated per run**
  (`cdc-bridge-{uuid4.hex[:8]}`), so each invocation reads from the earliest
  offset — this is intentional for one-shot DAG runs but produces duplicates
  if the same bridge is restarted while the topic still has data. Exit codes:
  `0` on graceful shutdown, `1` on Kafka connection failure.

### 4.4 Trino catalog — `scripts/trino_register.py`

```mermaid
flowchart TD
    WAIT["wait_for_trino()<br/>SELECT 1 polling<br/>timeout 300s"]
    SCHEMA["CREATE SCHEMA IF NOT EXISTS hive.nyc"]
    T1["DROP TABLE trips<br/>CREATE TABLE trips<br/>Parquet, partitioned by<br/>pickup_year/pickup_month<br/>29 columns"]
    T2["DROP TABLE invalid_trips<br/>CREATE TABLE invalid_trips<br/>Parquet, unpartitioned<br/>+ validation_errors ARRAY(VARCHAR)<br/>+ quarantine_ts"]
    T3["DROP TABLE taxi_zone_lookup<br/>CREATE TABLE taxi_zone_lookup<br/>CSV with header, 4 columns"]
    SYNC["CALL hive.system.sync_partition_metadata<br/>(schema_name='nyc',<br/>table_name='trips', mode='FULL')"]
    SMOKE["SELECT COUNT(*) × 3<br/>(trips, invalid_trips, taxi_zone_lookup)"]

    S1["s3a://nyc-silver/trips<br/>(SILVER_PATH env)"]
    S2["s3a://nyc-quarantine/invalid_trips<br/>(QUARANTINE_PATH env)"]
    S3["s3a://nyc-lookup/<br/>(ZONES_PATH env)"]

    WAIT --> SCHEMA --> T1
    SCHEMA --> T2
    SCHEMA --> T3
    S1 -.->|external_location| T1
    S2 -.->|external_location| T2
    S3 -.->|external_location| T3
    T1 --> SYNC --> SMOKE
    T2 --> SMOKE
    T3 --> SMOKE
```

- Idempotent: `DROP TABLE IF EXISTS` + `CREATE TABLE` for the 3 tables.
- `hive.nyc.trips` (Parquet, partitioned by `pickup_year/pickup_month`).
- `hive.nyc.invalid_trips` (Parquet, unpartitioned, includes
  `validation_errors ARRAY(VARCHAR)` and `quarantine_ts`).
- `hive.nyc.taxi_zone_lookup` (CSV with header).
- `CALL hive.system.sync_partition_metadata(schema_name => 'nyc',
  table_name => 'trips', mode => 'FULL')` after creation.
- Smoke test: `SELECT COUNT(*)` for all 3 tables.
- Default paths read from env vars `SILVER_PATH`, `QUARANTINE_PATH`,
  `ZONES_PATH` (overridable for S3 vs local FS).

### 4.5 Validation rules

10 rules are applied in batch; streaming adds an 11th for `event_id`.
Each rule produces a tagged string in the `validation_errors` array; rows
with empty arrays are written to silver, the rest to quarantine.

| # | Rule (batch error string) | Rule (streaming error string) |
|---|---|---|
| 1 | `pickup_datetime_null_or_invalid` | same |
| 2 | `dropoff_datetime_null_or_invalid` | same |
| 3 | `invalid_trip_duration` | same |
| 4 | `non_positive_trip_distance` | `trip_distance_must_be_gt_0` |
| 5 | `negative_fare_amount` | `fare_amount_must_be_gte_0` |
| 6 | `total_amount_less_than_fare` | `total_amount_must_be_gte_fare_amount` |
| 7 | `invalid_passenger_count` | `passenger_count_out_of_range` |
| 8 | `payment_type_out_of_range` | same |
| 9 | `unknown_pickup_location` | `pickup_location_not_found` |
| 10 | `unknown_dropoff_location` | `dropoff_location_not_found` |
| 11 | (n/a) | `event_id_null` (streaming only) |

The dbt staging layer additionally runs `nullif(nullif(...))` on the zone
columns (`Unknown`, `N/A`, `NV`) as a belt-and-suspenders clean.

### 4.6 dbt project — `dbt/`

```mermaid
flowchart TD
    subgraph SRC["Trino external tables"]
        T1["hive.nyc.trips<br/>(silver)"]
        T2["hive.nyc.invalid_trips"]
        T3["hive.nyc.taxi_zone_lookup"]
    end

    subgraph STG["models/staging/ — 3 views"]
        S1["stg_trips<br/>cast + nullif() zone"]
        S2["stg_zones"]
        S3["stg_invalid_trips<br/>cast + validation_errors"]
    end

    subgraph MARTS["models/marts/ — 8 views"]
        M1["fact_trips<br/>+ tip_rate, trip_duration_sec,<br/>pickup_dow, pickup_hour_ts"]
        M2["fact_invalid_trips<br/>cross join unnest(validation_errors)"]
        M3["dim_zone<br/>union pickup + dropoff zones"]
        M4["mart_hourly_summary"]
        M5["mart_revenue_by_day"]
        M6["mart_revenue_by_zone"]
        M7["mart_payment_type_summary"]
        M8["mart_trips_by_hour"]
    end

    subgraph GOLD["models/gold/ — 19 BI views"]
        G1["gold_fact_trips<br/>gold_dim_zone<br/>gold_dq_row_count_trend<br/>gold_validation_summary<br/>(4 base)"]
        G2["Customer · payment · tipping<br/>gold_customer_segments<br/>gold_customer_journey<br/>gold_payment_behavior<br/>gold_tipping_culture<br/>(4)"]
        G3["Executive · revenue · risk<br/>gold_executive_daily<br/>gold_executive_weekly<br/>gold_revenue_waterfall<br/>gold_risk_dashboard<br/>(4)"]
        G4["Operations · vendor · growth<br/>gold_trip_unit_economics<br/>gold_vendor_battlecard<br/>gold_zone_demand_heatmap<br/>gold_growth_metrics<br/>gold_hourly_pulse<br/>(5)"]
    end

    subgraph TESTS["tests/ — 27 tests"]
        T_SQL["6 singular SQL<br/>assert_minimum_rows<br/>assert_recent_data<br/>passenger_count_range<br/>payment_type_range<br/>total_not_less_than_fare<br/>trip_distance_positive"]
        T_YML["21 generic yml<br/>not_null + accepted_values<br/>in 3 yml files"]
    end

    T1 --> S1
    T2 --> S3
    T3 --> S2

    S1 --> M1
    S1 --> M3
    S1 --> M4
    S1 --> M5
    S1 --> M6
    S1 --> M7
    S1 --> M8
    S3 --> M2
    S2 --> M3

    M1 --> G1
    M3 --> G1
    M4 --> G2
    M5 --> G2
    M5 --> G3
    M1 --> G3
    M1 --> G4
    M2 --> G1

    MARTS -.-> TESTS
    GOLD -.-> TESTS
```

- 30 SQL models:
  - **Staging** (3) — `stg_trips`, `stg_zones`, `stg_invalid_trips`. All
    `materialized='view'`. Cast types and clean null-ish zone strings.
  - **Marts** (8) — `fact_trips`, `fact_invalid_trips`, `dim_zone`,
    `mart_hourly_summary`, `mart_revenue_by_day`, `mart_revenue_by_zone`,
    `mart_payment_type_summary`, `mart_trips_by_hour`. All
    `materialized='view'`.
  - **Gold** (19) — `gold_fact_trips`, `gold_dim_zone`,
    `gold_mart_revenue_by_day`, `gold_mart_revenue_by_zone`,
    `gold_dq_row_count_trend`, `gold_validation_summary`, plus 13 BI models
    (`gold_customer_segments`, `gold_customer_journey`,
    `gold_payment_behavior`, `gold_tipping_culture`,
    `gold_trip_unit_economics`, `gold_zone_demand_heatmap`,
    `gold_vendor_battlecard`, `gold_growth_metrics`,
    `gold_executive_daily`, `gold_executive_weekly`,
    `gold_revenue_waterfall`, `gold_hourly_pulse`, `gold_risk_dashboard`).
    All `materialized='view'` in dev.
- **27 tests**:
  - 6 singular SQL tests: `assert_minimum_rows`, `assert_recent_data`,
    `passenger_count_range`, `payment_type_range`,
    `total_not_less_than_fare`, `trip_distance_positive`.
  - 21 generic tests in yml files (`fact_invalid_trips_tests.yml`,
    `fact_trips_tests.yml`, `stg_trips_tests.yml`) — `not_null` and
    `accepted_values` checks.
- `profiles.yml` — `dev` target points at Trino (`type: trino`,
  `host: svc-trino`, `port: 8080`, `database: hive`, `schema: mart`,
  `threads: 4`).
- `dbt build` runs models + tests; expected `30 models, 27 tests`.

### 4.7 Gold export — `scripts/export_gold_to_minio.py`

```mermaid
flowchart TD
    WAIT["wait_for_trino()<br/>120s timeout"]
    SCHEMA["CREATE SCHEMA IF NOT EXISTS<br/>hive.nyc_gold"]
    LOOP{{"For each dataset in<br/>GOLD_DATASETS (33)"}}
    CT["SELECT COUNT(*) smoke test"]
    DROP["DROP TABLE IF EXISTS<br/>hive.nyc_gold.<name>"]
    CLEAN["clean_s3_path(bucket, prefix)<br/>recursive delete via minio client"]
    CTAS["CREATE TABLE hive.nyc_gold.<name><br/>WITH (external_location=<br/>'s3://nyc-gold/<name>/',<br/>format='PARQUET')<br/>AS <sql>"]
    RENAME["_add_parquet_extensions()<br/>rename data files to *.parquet"]
    NEXT["next dataset"]
    GOLD_OUT[("📦 s3://nyc-gold/<br/>33 datasets, Parquet")]
    EXIT{"All 33<br/>succeeded?"}
    OUT0["exit 0"]
    OUT1["exit 1<br/>(DAG task fails)"]

    WAIT --> SCHEMA --> LOOP
    LOOP --> CT --> DROP --> CLEAN --> CTAS --> RENAME --> NEXT
    NEXT --> LOOP
    LOOP -->|done| EXIT
    EXIT -->|Yes| OUT0
    EXIT -->|No| OUT1
    CTAS -.-> GOLD_OUT
```

- 33 datasets defined in `GOLD_DATASETS` list. Each entry: `{name, sql,
  location_subdir}`.
- For each dataset: `SELECT COUNT(*)` smoke test → `DROP TABLE IF EXISTS
  hive.nyc_gold.<name>` → `clean_s3_path` (recursively delete existing
  objects in the prefix) → `CREATE TABLE … WITH (external_location=…) AS
  <sql>`.
- Renames Parquet files to `.parquet` extension (Hive sometimes writes
  without an extension depending on partition layout).
- Returns exit 0 if all succeed, 1 otherwise.

### 4.8 Materialize to Postgres — `scripts/materialize_to_postgres.py`

```mermaid
flowchart TD
    WAIT_PG["wait_for_postgres()<br/>psycopg2 connect, 120s"]
    WAIT_TR["wait_for_trino()<br/>300s"]
    IMP["from export_gold_to_minio import GOLD_DATASETS<br/>(single source of truth — 33 SQLs)"]
    LOOP{{"For each dataset (33)"}}
    RUN_SQL["trino_cur.execute(sql)<br/>fetchall()"]
    MAP["TYPE_MAP trino → postgres<br/>(varchar→TEXT, double→DOUBLE PRECISION, …)"]
    CR["CREATE TABLE<br/><name>_new (col_defs)"]
    NAN["safe_rows:<br/>NaN → None"]
    INS["execute_values(<br/>page_size=5000)"]
    SWAP["DROP TABLE IF EXISTS <name><br/>ALTER TABLE <name>_new RENAME TO <name><br/>(atomic)"]
    NEXT["next dataset"]
    PG[("🐘 Postgres analytics<br/>nyc_analytics.public.*<br/>33 tables")]
    EXIT{"All 33<br/>succeeded?"}
    OUT0["exit 0"]
    OUT1["exit 1"]

    WAIT_PG --> IMP
    WAIT_TR --> IMP
    IMP --> LOOP
    LOOP --> RUN_SQL --> MAP --> CR --> NAN --> INS --> SWAP --> NEXT
    NEXT --> LOOP
    SWAP -.-> PG
    LOOP -->|done| EXIT
    EXIT -->|Yes| OUT0
    EXIT -->|No| OUT1
```

- Imports `GOLD_DATASETS` from `export_gold_to_minio.py` so the SQL is the
  same single source of truth.
- Per dataset: runs the same SQL against Trino, reads the result, maps
  Trino types to Postgres types (`TYPE_MAP`), creates `<name>_new`, batch
  inserts via `execute_values(page_size=5000)`, then atomic-swap:
  `DROP TABLE IF EXISTS <name>; ALTER TABLE <name>_new RENAME TO <name>`.
- Cleans `NaN` to `None` before insert.
- Returns exit 0 if all succeed, 1 if any dataset fails.

### 4.9 Superset — `scripts/superset_bootstrap.py` and
`scripts/superset_saved_queries.py`

```mermaid
flowchart TD
    subgraph BOOT["superset_bootstrap.py — idempotent"]
        LOGIN["POST /security/login<br/>get access_token"]
        DB["1 database<br/>'NYC Trino' → postgresql://<br/>svc-postgres-analytics"]
        DS["46 datasets<br/>(GOLD_TABLES list:<br/>33 base + 13 BI)"]
        CH["30 charts (CHART_DEFS)<br/>bar / line / pie / table<br/>big_number_total / dist_bar<br/>echarts_timeseries_bar/line"]
        DASH["1 dashboard<br/>'NYC Taxi Overview'<br/>slug=nyc-taxi"]
        LOGIN --> DB --> DS --> CH --> DASH
    end

    subgraph SQL["superset_saved_queries.py — idempotent"]
        SQL_LOGIN["POST /security/login"]
        LIST["GET /saved_query/"]
        CREATE["POST /saved_query/<br/>for each entry not yet present"]
        SQL_LOGIN --> LIST --> CREATE
    end

    subgraph TARGET["Targets"]
        T_PG[("🐘 Postgres<br/>nyc_analytics.public.*<br/>for charts/dashboard")]
        T_TR[("🔍 Trino<br/>hive.mart / hive.nyc_gold<br/>for saved queries")]
    end

    DB -.-> T_PG
    DS -.-> T_PG
    CH -.-> T_PG
    DASH -.-> T_PG
    CREATE -.-> T_TR

    BOOT -.->|"config: WTF_CSRF_ENABLED=False<br/>TALISMAN_ENABLED=False"| BOOT
```

- `superset_bootstrap.py` registers (idempotently):
  - 1 database (`NYC Trino` → `postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics`).
  - 46 datasets listed in `GOLD_TABLES` (33 base + 13 BI).
  - 30 chart definitions in `CHART_DEFS` (bar / line / pie / table /
    big-number / echarts timeseries / dist bar).
  - 1 dashboard `nyc-taxi`.
  - Uses `WTF_CSRF_ENABLED = False` and `TALISMAN_ENABLED = False` for
    bootstrap POST calls.
- `superset_saved_queries.py` registers 25 saved queries in SQL Lab
  targeting `hive.mart` and `hive.nyc_gold` schemas in Trino.

### 4.10 Anomaly check — `scripts/check_anomaly.py`

- Reads `hive.mart.gold_dq_row_count_trend` and flags rows with
  `anomaly_flag != 'NORMAL'`.
- Prints anomalies and a summary (`low`, `high`, `total_days`).
- **Exit code 0 always** — informational only, does not block the DAG.

---

## 5. Airflow DAGs

All three DAGs use `KubernetesPodOperator` (KPO) with these shared defaults:

- `namespace="nyc-taxi"`, `in_cluster=True`, `service_account_name="airflow-sa"`
- `get_logs=True` (stream pod logs into Airflow task log)
- `volumes=[project_volume]` mounting `project-files-pvc` at `/opt/project`
  (carries `airflow/dags/`, `jobs/`, `scripts/`, `dbt/`, `charts/`)
- `image_pull_policy="IfNotPresent"` (uses locally loaded kind images, no
  registry pull)
- `security_context=run_as_user=0` for Spark tasks (so the S3A filesystem
  can write to PVCs)

### 5.0 Common task configuration

| Setting | `nyc_e2e_pipeline` | `nyc_analytics_refresh` | `nyc_cdc_pipeline` |
|---|---|---|---|
| `retries` | 3 | 3 | 2 |
| `retry_delay` | 30s | 30s | 30s |
| `execution_timeout` | 30 min | 30 min | 15 min |
| `depends_on_past` | `False` | `False` | `False` |
| `max_active_runs` | 1 | 1 | 1 |
| `catchup` | (default) | `False` | `False` |
| Schedule | `None` (manual) | `@weekly` | `None` (manual) |
| `start_date` | 2024-01-01 | 2026-01-01 | 2024-01-01 |

### 5.1 `nyc_e2e_pipeline` — 13 tasks, manual trigger

The full E2E pipeline: optional CDC chain (seed → register → bridge) feeds
events into the streaming path; batch and stream both converge at Trino;
dbt builds the views; gold + materialize run in parallel; Superset is
re-bootstrapped; the analytics question suite closes the run.

```mermaid
flowchart TD
    cdc_seed([cdc_seed]) --> cdc_register([cdc_register]) --> cdc_bridge([cdc_bridge])
    cdc_bridge --> spark_streaming([spark_streaming])

    spark_batch([spark_batch])

    spark_streaming --> trino_bootstrap{{"trino_bootstrap<br/><i>trigger_rule=one_success</i>"}}
    spark_batch --> trino_bootstrap

    trino_bootstrap --> dbt_build([dbt_build])

    dbt_build --> gold_export([gold_export])
    dbt_build --> anomaly_check([anomaly_check])
    dbt_build --> materialize_postgres([materialize_postgres])

    materialize_postgres --> superset_bootstrap([superset_bootstrap])
    superset_bootstrap --> superset_saved_queries([superset_saved_queries])
    superset_saved_queries --> analytics_check([analytics_check])

    classDef branch fill:#fff5e1,stroke:#c94,color:#222
    class gold_export,anomaly_check branch
```

**Trigger rule**: `trino_bootstrap` uses `trigger_rule="one_success"` — it
runs if **either** `spark_batch` or `spark_streaming` succeeded. This lets
you run just the batch path, just the CDC path, or both, without rewiring.
The rest of the DAG uses the default `all_success`.

**Pod retention**: `trino_bootstrap` and `cdc_seed` set
`is_delete_operator_pod=False` so their pod logs survive after the task
completes (useful for debugging S3A / Trino bootstrap failures). The other
11 tasks use the default `True` and clean up.

**`trino_bootstrap` startup**: `startup_timeout_seconds=600` — Trino may
take a while to become ready the first time after a fresh Helm deploy.

#### Task details

| Task | Image | Command / Args | Volumes (extra) | Env vars (extra) | Notes |
|---|---|---|---|---|---|
| `cdc_seed` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-seed --input /opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --max-rows 1000 --dsn postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi` | `raw-data-pvc` → `/mnt/nyc-data` | — | `is_delete_operator_pod=False` |
| `cdc_register` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-register --debezium-url http://svc-debezium:8083 --postgres-host svc-postgres-cdc` | — | — | Idempotent (DELETE + POST) |
| `cdc_bridge` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-bridge --bootstrap-server svc-kafka:9092 --input-topic nyc_cdc.public.trips --output-topic taxi.trip.events --idle-timeout 30 --flush-interval 500` | — | — | Exits 30s after no messages; benchmark at end |
| `spark_batch` | `apache/spark:3.5.1` | `spark-submit --master local[*] --packages hadoop-aws:3.3.4,aws-java-sdk-bundle:1.12.262 --conf spark.jars.ivy=/opt/project/.ivy2 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.scheduler.mode=FAIR /opt/project/jobs/spark_local_batch.py --input s3a://nyc-raw/yellow_taxi/year=*/month=*/*.parquet --lookup s3a://nyc-lookup/taxi_zone_lookup.csv --silver s3a://nyc-silver/trips --quarantine s3a://nyc-quarantine/invalid_trips --incremental` | — | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | `security_context=run_as_user=0` |
| `spark_streaming` | `apache/spark:3.5.1` | `spark-submit --master local[*] --conf spark.jars.ivy=/opt/project/.ivy2 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.scheduler.mode=FAIR --packages spark-sql-kafka-0-10_2.12:3.5.1,hadoop-aws:3.3.4,aws-java-sdk-bundle:1.12.262 /opt/project/jobs/spark_stream_taxi_events.py --bootstrap-server svc-kafka:9092 --topic taxi.trip.events --lookup-path s3a://nyc-lookup/taxi_zone_lookup.csv --silver-path s3a://nyc-silver/trips --quarantine-path s3a://nyc-quarantine/invalid_trips --checkpoint-path s3a://nyc-silver/checkpoints/spark_stream_taxi_events/taxi.trip.events --trigger-available-now` | — | same as `spark_batch` | `security_context=run_as_user=0`; one-shot via `--trigger-available-now` |
| `trino_bootstrap` | `nyc-pipeline-tools:latest` | `entrypoint-trino-bootstrap` | — | `TRINO_HOST=svc-trino`, `TRINO_PORT=8080`, `TRINO_USE_SSL=false`, `S3_MODE=true`, `AWS_ACCESS_KEY_ID=minio`, `AWS_SECRET_ACCESS_KEY=minio123`, `AWS_ENDPOINT_URL=http://svc-minio:9000`, `SILVER_PATH=s3://nyc-silver/trips`, `QUARANTINE_PATH=s3://nyc-quarantine/invalid_trips`, `ZONES_PATH=s3://nyc-lookup/` | `is_delete_operator_pod=False`; `startup_timeout_seconds=600`; `trigger_rule="one_success"` |
| `dbt_build` | `nyc-dbt:latest` | `entrypoint-dbt` | — | `DBT_PROFILES_DIR=/opt/project/dbt`, `TRINO_HOST=svc-trino` | Runs `trino_sync_partitions.py` first, then `dbt build` (30 models + 27 tests) |
| `gold_export` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/export_gold_to_minio.py` | — | `TRINO_HOST=svc-trino`, `TRINO_PORT=8080` | 33 datasets via CTAS to s3://nyc-gold/ |
| `materialize_postgres` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/materialize_to_postgres.py` | — | `TRINO_HOST`, `TRINO_PORT`, `PG_ANALYTICS_HOST=svc-postgres-analytics`, `PG_ANALYTICS_USER=analytics`, `PG_ANALYTICS_PASSWORD=analytics`, `PG_ANALYTICS_DB=nyc_analytics` | Atomic swap for 33 tables |
| `superset_bootstrap` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_bootstrap.py` | — | `SUPERSET_URL=http://svc-superset:8088`, `PG_ANALYTICS_URI=postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics` | 1 DB + 46 datasets + 30 charts + 1 dashboard |
| `superset_saved_queries` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_saved_queries.py` | — | `SUPERSET_URL=http://svc-superset:8088` | 25 saved queries targeting Trino |
| `analytics_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/run_analytics_questions.py` | — | `TRINO_HOST`, `TRINO_PORT` | 10 SQL questions from `sql/analytics_questions.sql`; each must return ≥1 row |
| `anomaly_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/check_anomaly.py` | — | `TRINO_HOST`, `TRINO_PORT` | Reads `gold_dq_row_count_trend`; informational, exit 0 always |

**Image override note**: when running the DAG, the `--incremental` flag on
`spark_batch` means it will skip the partition if `max(pickup_year),
max(pickup_month)` in silver already covers the input files. This is why
the DAG can be re-triggered safely without duplicating rows.

### 5.2 `nyc_analytics_refresh` — 7 tasks, `@weekly`

Re-runs the analytics layer assuming the silver layer is already populated
(no Spark task — they are assumed to have run via the E2E DAG or
manually). Reuses the same scripts and image pattern as the E2E DAG.

```mermaid
flowchart TD
    dbt_build([dbt_build]) --> materialize_postgres([materialize_postgres])
    dbt_build --> gold_export([gold_export])
    dbt_build --> anomaly_check([anomaly_check])

    materialize_postgres --> superset_bootstrap([superset_bootstrap])
    superset_bootstrap --> superset_saved_queries([superset_saved_queries])
    superset_saved_queries --> analytics_check([analytics_check])

    classDef branch fill:#fff5e1,stroke:#c94,color:#222
    class gold_export,anomaly_check branch
```

**Fan-out semantics**: `dbt_build` has 3 downstream tasks:

- `materialize_postgres` — starts the materialize chain.
- `gold_export` — runs in parallel, no downstream (just writes s3://nyc-gold/).
- `anomaly_check` — runs in parallel, no downstream.

`gold_export` and `anomaly_check` are **branch ends**: they don't block
each other and don't block the `materialize_postgres` chain. This means
a `gold_export` failure does NOT cause `analytics_check` to skip; the
DAG only fails the analytics path if `materialize_postgres` or one of
the Superset tasks fails.

**Task details** (all KPO, all use the same env vars as in §5.1 unless
overridden):

| Task | Image | Command / Args | Notes |
|---|---|---|---|
| `dbt_build` | `nyc-dbt:latest` | `entrypoint-dbt` | Same as §5.1 |
| `materialize_postgres` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/materialize_to_postgres.py` | Same as §5.1 |
| `gold_export` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/export_gold_to_minio.py` | Same as §5.1 |
| `anomaly_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/check_anomaly.py` | Same as §5.1 |
| `superset_bootstrap` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_bootstrap.py` | Same as §5.1 |
| `superset_saved_queries` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_saved_queries.py` | Same as §5.1 |
| `analytics_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/run_analytics_questions.py` | Same as §5.1 |

**Pod retention**: unlike §5.1, no task in this DAG sets
`is_delete_operator_pod=False`. Logs are deleted on completion by default.

### 5.3 `nyc_cdc_pipeline` — 3 tasks, manual

Used to demo the CDC chain end-to-end without running the full E2E. Seeds
Postgres with 5000 rows from a Parquet file, registers the Debezium
connector, then bridges CDC events into the standard taxi event topic for
~30 seconds before exiting.

```mermaid
flowchart LR
    cdc_seed([cdc_seed]) --> cdc_register([cdc_register]) --> cdc_bridge([cdc_bridge])
```

**Task details**:

| Task | Image | Command / Args | Notes |
|---|---|---|---|
| `cdc_seed` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-seed --input /opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --max-rows 5000 --dsn postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi` | 5000 rows sampled with `random_state=42` |
| `cdc_register` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-register --debezium-url http://svc-debezium:8083 --postgres-host svc-postgres-cdc` | Idempotent |
| `cdc_bridge` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-bridge --bootstrap-server svc-kafka:9092 --input-topic nyc_cdc.public.trips --output-topic taxi.trip.events --idle-timeout 30 --flush-interval 500` | `--idle-timeout 30` makes it self-terminate after 30s of no messages; not `is_delete_operator_pod=False` |

This DAG does **not** trigger any Spark task. After `cdc_bridge` exits,
the events it produced sit in the `taxi.trip.events` topic. To consume
them you must run `spark_streaming` separately (either via the E2E DAG's
task or `make spark-streaming`).

### 5.4 Common behaviors and gotchas

- **No `depends_on_past=True`** in any task. You can clear and re-run a
  single task without affecting downstream tasks' state.
- **`startup_timeout_seconds=600`** is set only on `trino_bootstrap`. All
  other tasks use the KPO default of 120s.
- **`is_delete_operator_pod`** is `False` only on `trino_bootstrap` and
  `cdc_seed` (both in `nyc_e2e_pipeline`). When debugging these, their
  pods persist after the task completes; everything else is auto-cleaned.
- **ServiceAccount**: `airflow-sa` (defined in
  `charts/nyc-taxi/templates/airflow/rbac.yaml`). Has `get/list/watch`
  and `create/delete` permissions on pods and pod logs.
- **Image references** assume images are loaded into the local kind
  cluster (Skaffold pre-deploy hook handles this). If you run against a
  remote cluster, change `image_pull_policy` to `Always` and push the
  images to a registry.
- **Restart semantics**: failed tasks retry 3× (2× for CDC) with a 30s
  backoff. After 3 failures the task is marked failed and the DAG run
  fails; downstream tasks are skipped.

---

## 6. Deployment (Kubernetes / Skaffold — primary)

### 6.1 One-command deploy

```bash
# First time only:
bash scripts/cluster_up.sh          # creates kind cluster + loads public images

# Every time:
skaffold dev --namespace nyc-taxi   # builds images, deploys Helm, port-forwards, watches
```

Skaffold:
- Builds 4 images: `nyc-pipeline-tools`, `nyc-dbt`, `nyc-airflow`, `nyc-superset`.
- Pre-deploy hook: deletes immutable jobs in the namespace, tar-syncs
  `airflow/dags/`, `jobs/`, `scripts/`, `dbt/`, `charts/` to the
  `project-files-pvc` hostPath on the `kind-worker` node.
- Helm deploys the chart at `charts/nyc-taxi/` (44 valid K8s manifests in
  13 component folders).
- Port-forwards 8 services to `localhost:39080-39087`.

### 6.2 Service port-forwards

| Service | URL | Credentials |
|---|---|---|
| Superset | `http://localhost:39080` | `admin` / `admin` |
| MinIO API | `http://localhost:39081` | `minio` / `minio123` |
| Kafka UI | `http://localhost:39082` | — |
| Spark Master | `http://localhost:39083` | — |
| Trino | `http://localhost:39084` | — |
| Airflow | `http://localhost:39085` | `admin` / `admin` |
| MinIO Console | `http://localhost:39086` | `minio` / `minio123` |
| Postgres CDC | `http://localhost:39087` | `postgres` / `postgres` |

### 6.3 K8s service names (mandatory `svc-` prefix)

Inside the cluster, all service names carry a `svc-` prefix. Code in this
repo uses those names directly:

- `svc-minio:9000` (S3A + Trino Hive connector)
- `svc-trino:8080` (Trino JDBC)
- `svc-kafka:9092` (Spark streaming)
- `svc-postgres-cdc:5432` (Debezium + cdc_seed)
- `svc-postgres-analytics:5432` (materialize + Superset)
- `svc-debezium:8083` (cdc_register REST API)
- `svc-superset:8088` (Superset API)
- `svc-airflow-webserver:8080` (Airflow web UI)

### 6.4 K8s resources created by the chart

| Kind | Count | Examples |
|---|---|---|
| `Deployment` | 8 | kafka-ui, superset, trino, deb, airflow-webserver, airflow-scheduler, spark-master, spark-worker |
| `StatefulSet` | 4 | zookeeper, kafka, postgres-cdc, postgres-analytics, airflow-postgres |
| `Service` | 12 | svc-* for every component above |
| `Job` | 4 | topic-init, postgres-init, minio-setup, airflow-init |
| `ConfigMap` | 3 | airflow entrypoint, superset config, trino config |
| `PersistentVolume` / `PVC` | 5 | project-files-pv, raw-data-pv, minio-data, postgres-cdc, postgres-analytics, airflow-postgres |
| `ServiceAccount` / `Role` / `RoleBinding` | 1 set | airflow-sa |
| `Namespace` | 1 | nyc-taxi |

> Note: the chart source contains 3 extra template files at
> `charts/nyc-taxi/templates/trino/charts/nyc-taxi/templates/trino/*.yaml`
> that are not loaded by Helm due to the nested path. They are duplicates
> of the correct templates and can be deleted.

### 6.5 Trino configuration (current)

- `docker/trino/etc/jvm.config`: `-Xmx2G` (2 GB heap, G1GC, 32 MB region).
- `docker/trino/etc/config.properties`: single-node coordinator, port 8080.
- `docker/trino/etc/catalog/hive.properties`: file-based metastore at
  `/data/trino-metastore`, `hive.recursive-directories=true`,
  `hive.s3.endpoint=http://minio:9000` (Compose) or `svc-minio:9000` (K8s),
  `hive.s3.path-style-access=true`, `hive.ssl.enabled=false`,
  `hive.non-managed-table-creates-enabled=true`,
  `hive.non-managed-table-writes-enabled=true`.

### 6.6 Postgres CDC configuration (current)

- Image: `postgres:16-alpine`.
- Replication args: `wal_level=logical`, `max_replication_slots=4`,
  `max_wal_senders=4`.
- StatefulSet with PVC (no backup, no HA in dev).
- `POSTGRES_PASSWORD=postgres`, `POSTGRES_USER=postgres`,
  `POSTGRES_DB=nyc_taxi`.

### 6.7 Kafka configuration (current)

- Image: `confluentinc/cp-kafka` (7.x).
- Single broker, single pod.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`,
  `KAFKA_TRANSACTION_STATE_LOG_*_REPLICATION_FACTOR=1`.
- Topics created at startup by `scripts/create_kafka_topics.py`:
  `taxi.trip.events` (3 partitions), `taxi.trip.invalid` (3 partitions),
  `taxi.trip.dlq` (3 partitions).

---

## 7. Local development (Docker Compose)

```bash
make infra-up           # start ZK + Kafka + MinIO + Spark
make kafka-topics       # create topics
make spark-batch MONTH=03
make trino-bootstrap
make dbt-build
make gold-export
make superset-bootstrap
make verify-all         # 7-step E2E
make clean-all
```

29 Makefile targets total. Compose stack has 16 services, 6 profile groups
(`default`, `tools`, `trino`, `dbt`, `superset`, `airflow`).

Compose-mode ports (different from K8s):

| Service | Port |
|---|---|
| Superset | 8088 |
| MinIO Console | 9001 |
| Airflow | 8085 |
| Kafka UI | 8080 |
| Spark Master | 8081 |
| Spark Worker | 8082 |
| Trino (JDBC) | 8083 |

---

## 8. Validation

After a full pipeline run, the following checks are expected to pass:

| Check | How to run | Expected |
|---|---|---|
| `dbt build` | `make dbt-build` or `nyc_analytics_refresh` DAG | 30 models, 27 tests, 0 errors |
| Mart row counts | `make verify-mart` or `scripts/verify_mart.py` | `dim_zone ≈ 261`, `fact_trips ≈ 8.4M`, `mart_hourly_summary ≈ 11K`, `mart_revenue_by_day ≈ 90` |
| Analytics questions | `make verify-analytics` or `scripts/run_analytics_questions.py` | 10/10 PASS (each query returns ≥1 row) |
| CDC pipeline | `make verify-cdc` | Postgres row count > 0, Debezium connector RUNNING |
| Anomaly check | `scripts/check_anomaly.py` (DAG task) | informational, exit 0 |

`run_analytics_questions.py` reads `sql/analytics_questions.sql`, splits on
`-- N)` title markers, executes each question against Trino, and asserts
that each one returns at least one row. It is `bin/--incompatible with
multiple semicolons in the same question` so each `;` is a statement
boundary; questions must end with `;`.

---

## 9. Known limitations

These are characteristics of the code as of this README; the project is a
demo, not a production-ready platform.

### 9.1 Validation error strings differ between batch and streaming

The same logical rule produces different error strings in
`jobs/spark_local_batch.py` (e.g. `non_positive_trip_distance`) versus
`jobs/spark_stream_taxi_events.py` (e.g. `trip_distance_must_be_gt_0`).
Five rules diverge. A future cleanup should align them and decide on one
canonical naming convention.

### 9.2 Batch and streaming write to the same silver path

Both jobs write to `s3a://nyc-silver/trips/`. There is no column or
metadata marker distinguishing a row's source. This makes it impossible
to monitor batch and stream outputs independently or to roll one back
without affecting the other.

### 9.3 CDC bridge generates a new consumer group per run

`scripts/cdc_bridge.py` uses `group_id = f"cdc-bridge-{uuid4().hex[:8]}"`,
so every invocation starts from the earliest offset. This is fine for
one-shot DAG runs (the bridge exits after `--idle-timeout` seconds), but
it duplicates events if the bridge is restarted while the source topic
still has un-consumed data.

### 9.4 No quality gates in the main DAG

There is no `verify_silver` / `verify_gold` / `verify_postgres` /
`verify_superset` / `verify_freshness` task in any DAG. `analytics_check`
and `anomaly_check` are informational (exit 0 always).

### 9.5 Trino runs on a single node with file-based metastore

- Single coordinator (no HA).
- Metastore: file-based at `/opt/project/data/trino-metastore` (PVC). If
  the PVC is lost, the catalog must be re-created by re-running
  `scripts/trino_register.py`.
- JVM heap: `-Xmx2G` — large CTAS queries can OOM.
- `gold_export` runs 33 sequential CTAS. A `clean_s3_path` step is
  included to recover from partial failures, but a single Trino OOM can
  still force a retry of the full task.

### 9.6 Helm chart has nested template duplicates

`charts/nyc-taxi/templates/trino/charts/nyc-taxi/templates/trino/` contains
3 duplicate files (configmap, deployment, service) that Helm does not
load because of the path. They can be deleted; the real templates are at
`charts/nyc-taxi/templates/trino/*.yaml`.

### 9.7 Spark streaming uses `failOnDataLoss=false`

If Kafka retention drops a message between Spark runs, the silent
skipping hides the data loss. Production should switch this to `true` and
operate a longer retention topic.

### 9.8 No CDC chain monitoring

There is no DAG or task that polls Debezium connector status, Kafka
consumer lag, or Postgres replication slot lag. Failures in the CDC chain
are only detectable indirectly through `anomaly_check` or by inspecting
`gold_dq_row_count_trend`.

---
## 10. Roadmap / planned (not yet implemented)

This section documents what the design draft (`docs_update.md` + `new_doc.md`)
proposes but is **not** in the current code. Each item is meant to be
implemented in a separate change with its own tests.

### 10.1 What exists today vs. what is planned

The current pipeline runs as a single, mostly-monitored DAG. Quality
gates and continuous monitoring are documented in the design draft but
have not been implemented yet.

```mermaid
flowchart LR
    subgraph TODAY["Today (code actually running)"]
        direction LR
        T1["spark_batch + spark_streaming"]
        T2["trino_bootstrap"]
        T3["dbt_build"]
        T4["gold_export"]
        T5["materialize_to_postgres"]
        T6["superset_bootstrap"]
        T7["anomaly_check<br/>(exit 0 always)"]
        T8["analytics_check<br/>(10 SQL questions)"]
        T1 --> T2 --> T3
        T3 --> T4
        T3 --> T5 --> T6
        T3 --> T7
        T6 --> T8
    end

    subgraph TOMORROW["Planned (in design, not in code)"]
        direction LR
        P0["validate_raw_files<br/>[PLANNED #5]"]
        P1A["verify_silver"]
        P1B["verify_gold"]
        P1C["verify_postgres"]
        P1D["verify_superset"]
        P1E["verify_freshness"]
        P2A["monitor_dag @5min<br/>check_silver/gold/postgres/<br/>superset/freshness/<br/>pg_cdc/debezium/kafka"]
        P3A["pipeline_health.checks<br/>(Postgres table)"]
        P3B["Superset Pipeline<br/>Health Dashboard"]
        P4A["Slack + Email alerts"]
        P0 -.->|"block on fail"| T1
        T1 -.->|"gates"| P1A
        T4 -.->|"gates"| P1B
        T5 -.->|"gates"| P1C
        T6 -.->|"gates"| P1D
        T8 -.->|"gates"| P1E
        P2A -->|"write"| P3A -->|"read"| P3B
        P1A & P1B & P1C & P1D & P1E & P2A -.->|"FAIL"| P4A
    end

    style TODAY fill:#e8f5e9,stroke:#2e7d32
    style TOMORROW fill:#fff3e0,stroke:#c94,stroke-dasharray:5 3
```

**Legend**:

- **Solid lines** = code actually running today.
- **Dashed lines** = not yet implemented; the design draft proposes these.
- **Green box** = current main DAG (no quality gates, no continuous
  monitor).
- **Orange box** = planned additions (5 verify gates + 1 monitor DAG + 1
  health table + 1 health dashboard + 1 alert channel).

### 10.2 The five quality gates (planned)

```mermaid
flowchart LR
    subgraph PIPELINE["Main pipeline (nyc_e2e_pipeline)"]
        BATCH["spark_batch"] --> V1
        STREAM["spark_streaming"] --> V1
        V1["verify_silver<br/>check row_count > 0<br/>check null ratio = 0<br/>check AVG(distance) in range<br/>check MAX(date) fresh"] -->|PASS| TB["trino_bootstrap"]
        V1 -->|FAIL| BLOCK["BLOCK downstream<br/>+ Slack + Email"]
        TB --> DBT["dbt_build"]
        DBT --> GE["gold_export"] --> V2["verify_gold<br/>check 30/30 tables exist<br/>check row_count > 0<br/>check match dbt source"]
        V2 -->|FAIL| BLOCK
        DBT --> MP["materialize_postgres"] --> V3["verify_postgres<br/>check pg rows = gold rows<br/>check all 33 tables match"]
        V3 -->|PASS| SUP["superset_bootstrap"]
        V3 -->|FAIL| BLOCK
        SUP --> V4["verify_superset<br/>check charts render OK<br/>check metrics match Trino"]
        V4 -->|PASS| SSQ["superset_saved_queries"]
        V4 -->|FAIL| SLACK["Slack warning only"]
        SSQ --> V5["verify_freshness<br/>check MAX(date) le 35d<br/>check rows in 7d range"]
        V5 -->|PASS| AC["analytics_check"]
        V5 -->|FAIL| BLOCK
    end

    style V1 fill:#ffebee,stroke:#c00
    style V2 fill:#ffebee,stroke:#c00
    style V3 fill:#ffebee,stroke:#c00
    style V4 fill:#fff3e0,stroke:#c94
    style V5 fill:#ffebee,stroke:#c00
    style BLOCK fill:#c00,color:#fff
    style SLACK fill:#ff9800,color:#fff
```

Gate semantics:

| Gate | Reads from | Blocks | Triggered by | Slack on fail |
|---|---|---|---|---|
| `verify_silver` | Trino `hive.nyc.trips` | `trino_bootstrap` | every DAG run | yes + Email |
| `verify_gold` | Trino `hive.nyc_gold.*` | (none yet) | every DAG run | yes + Email |
| `verify_postgres` | Postgres `nyc_analytics.public.*` | `superset_bootstrap` | every DAG run | yes + Email |
| `verify_superset` | Superset API | (none — Superset is the last step) | every DAG run | yes (warning) |
| `verify_freshness` | Trino `hive.mart.fact_trips` | `analytics_check` | every DAG run | yes + Email |

`verify_superset` is a warning rather than a block because the dashboard
is the last step in the pipeline — there is nothing downstream to block.

### 10.3 The Monitor DAG (planned)

```mermaid
flowchart TD
    TRIG["TRIG: @every 5 minutes"]
    subgraph CHECKS["8 read-only checks (no writes)"]
        C1["check_silver<br/>row count, null ratio, distribution"]
        C2["check_gold<br/>30 tables, row counts"]
        C3["check_postgres<br/>pg rows = gold rows"]
        C4["check_superset<br/>charts render OK"]
        C5["check_freshness<br/>MAX(date) le 35d"]
        C6["check_pg_cdc<br/>WAL size, replication slot"]
        C7["check_debezium<br/>connector RUNNING? lag < 5 min?"]
        C8["check_kafka<br/>broker health, consumer lag"]
    end
    AGG["Aggregate to PASS / WARN / FAIL"]
    DB[("Postgres<br/>pipeline_health.checks")]
    DASH["Superset Pipeline<br/>Health Dashboard"]
    ALERT["Slack + Email<br/>(CRITICAL only)"]

    TRIG --> C1 & C2 & C3 & C4 & C5 & C6 & C7 & C8
    C1 & C2 & C3 & C4 & C5 & C6 & C7 & C8 --> AGG
    AGG --> DB
    DB --> DASH
    AGG -->|FAIL| ALERT
```

Key properties:

- **Read-only** — never writes to the data lake or the catalogs. Safe to
  run in parallel with the main pipeline.
- **Independent** — Monitor DAG failure does not block or affect the main
  pipeline. Airflow treats the two DAGs as independent runs.
- **Fast** — Each check is a single SELECT or REST call. Total runtime
  expected to be under 30 seconds.
- **Granular** — Each check has its own severity (PASS / WARN / FAIL) and
  its own alert channel.

### 10.4 Cross-node reconciliation (planned)

```mermaid
flowchart TD
    subgraph RECON["4 reconciliation checks (block on mismatch)"]
        R1["R1: spark_input_rows<br/>== silver_rows + quarantine_rows"]
        R2["R2: silver_rows<br/>== mart.fact_trips_rows"]
        R3["R3: mart.* rows<br/>== gold_export.* rows"]
        R4["R4: gold.* rows<br/>== postgres.* rows"]
    end
    S1["spark_batch output"]
    SIL["nyc-silver"]
    QUA["nyc-quarantine"]
    MARTS["hive.mart.*"]
    GOLD["nyc-gold/*"]
    PG["Postgres analytics"]
    OK["ALL CLEAR<br/>Superset safe to display"]
    BLOCK["BLOCK + Slack + Email"]

    S1 & SIL & QUA --> R1
    R1 -->|PASS| R2
    R1 -->|FAIL| BLOCK
    SIL & MARTS --> R2
    R2 -->|PASS| R3
    R2 -->|FAIL| BLOCK
    MARTS & GOLD --> R3
    R3 -->|PASS| R4
    R3 -->|FAIL| BLOCK
    GOLD & PG --> R4
    R4 -->|PASS| OK
    R4 -->|FAIL| BLOCK

    style BLOCK fill:#c00,color:#fff
    style OK fill:#4caf50,color:#fff
```

Each `R*` runs as a KubernetesPodOperator task between the corresponding
pipeline tasks. Row counts are compared via `SELECT COUNT(*)` on each
side; tolerance is 0 (exact match).

### 10.5 Itemized roadmap (16 items)

The 16 items below are grouped by which pipeline layer they affect.
Effort estimates are for a single developer working part-time.

| # | Feature | Layer | Source | Effort |
|---|---|---|---|---|
| | **Quality gates (group A)** | | | |
| 1 | **Quality gates** — 5 inline `verify_*` tasks (`verify_silver`, `verify_gold`, `verify_postgres`, `verify_superset`, `verify_freshness`) that block the DAG on failure | main DAG | `docs_update.md` §2 | 1 week |
| 3 | **Cross-node reconciliation** — 4 row-count reconciliation checks (spark_input == silver + quarantine, silver == mart.fact_trips, mart.* == gold.*, gold.* == postgres.*) | main DAG | `docs_update.md` §4 | 3 days |
| 13 | **Output contracts YAML** — declarative per-node contracts (`contracts/silver.yaml`, `contracts/gold.yaml`, …) consumed by the verify tasks | main DAG | `AGENTS.md` Output Contracts | 3 days |
| | **Monitoring (group B)** | | | |
| 2 | **Monitor DAG** — separate DAG running every 5 minutes with 8 read-only checks (silver / gold / postgres / superset / freshness / pg_cdc / debezium / kafka) → Slack + Email on failure | monitor DAG | `docs_update.md` §2 | 1 week |
| 4 | **Pipeline Health Dashboard** — Superset dashboard on a `pipeline_health.checks` table populated every 5 minutes by the Monitor DAG | monitor DAG | `AGENTS.md` Health Dashboard section | 1 week |
| 12 | **Alert integration** — Slack + Email on DAG failure, OOM, anomaly detection, freshness violation | monitor DAG | `AGENTS.md` Failure Mode Coverage + `docs/15-dataops-roadmap.md` | 1 week |
| | **Data integrity (group C)** | | | |
| 5 | **MinIO pre-ingest validation** — `scripts/validate_raw_files.py` that quarantines malformed Parquet before `spark_batch` runs | ingest | `docs_update.md` §12 | 3 days |
| 14 | **Spark batch crash recovery** — write to `_tmp/` and atomically move to `silver/trips/` on success; check row count after retry | ingest | `docs_update.md` Spark Scalability section | 3 days |
| 15 | **Spark batch auto-split by month** — when the pod has < 8 GB RAM, split into 12 sequential sub-tasks instead of one big `local[*]` | ingest | `docs_update.md` Spark Scalability section | 3 days |
| | **CDC chain (group D)** | | | |
| 6 | **CDC chain hardening** — Postgres WAL size + slot monitor, Debezium `snapshot.mode=schema_only` + `delete.handling.mode=rewrite`, Kafka idempotent producer + `enable.idempotence` + longer retention + DLQ, cdc_bridge fixed group_id + `auto.offset.reset=latest` | CDC | `docs_update.md` §8–11 | 1 week |
| | **Compute (group E)** | | | |
| 7 | **Trino resource groups** — `gold_export` capped at 2 concurrent × 3 GB, `adhoc` at 3 × 2 GB; batched CTAS (3 × 10) with 30s pause between batches | catalog | `docs_update.md` §13 | 3 days |
| 8 | **dbt CI + incremental models + business tests** — GitHub Actions running `dbt build --target staging` on PRs, `fact_trips` materialized as `incremental`, cross-model revenue assertions | transform | `docs_update.md` §14 | 1 week |
| | **Presentation (group F)** | | | |
| 9 | **Superset hardening** — idempotent bootstrap (already mostly true), cache-bust API after pipeline, chart SQL version control in `superset/charts/`, secret rotation via env vars, dashboard export to git | serve | `docs_update.md` §15 | 3 days |
| 10 | **Anomaly check upgrade** — multi-metric (fare / distance / revenue), 30-day rolling ± 3σ baseline, optional `--block` flag, Slack webhook | serve | `docs_update.md` §16 | 2 days |
| | **Infrastructure (group G)** | | | |
| 11 | **Staging environment** — separate namespace + MinIO bucket + Postgres for pre-merge testing | infra | `docs/15-dataops-roadmap.md` | 2 weeks |
| 16 | **Production deployment** — Trino 1 coord + 2 workers, Kafka 3 brokers, Postgres CDC on RDS Multi-AZ, MinIO → S3, Spark on EMR/Glue | infra | `new_doc.md` Production Hardening + `docs_update.md` §8 | 2+ weeks |

**Effort totals** (group sum):

- Group A (gates + contracts): ~3 weeks
- Group B (monitor + dashboard + alerts): ~3 weeks
- Group C (ingest hardening): ~1.5 weeks
- Group D (CDC chain): ~1 week
- Group E (Trino + dbt): ~2 weeks
- Group F (Superset + anomaly): ~1 week
- Group G (infra + production): ~4+ weeks
- **Total: ~15 weeks** for a single part-time developer
### 10.6 Known gaps the roadmap does not address yet

These are open issues tracked in `docs/issues.md` but not yet scheduled.
They fall outside the 16 items above and would need separate discussion:

+- `is_delete_operator_pod=False` is set on only 2 of 13 DAG tasks
  (`trino_bootstrap` and `cdc_seed`); the other 11 still drop their pod
  logs on completion. (§5.4 lists which ones.)
+- The CDC bridge's random `group_id` produces duplicate events on
  restart (see §9.3). Item #6 fixes part of this but not the
  `group_id` choice itself.
+- Streaming and batch share a single silver path with no source marker
  (see §9.2). The roadmap proposes `nyc-silver/batch/trips` and
  `nyc-silver/stream/trips` separation, but only as part of item #6
  (CDC hardening) and is not formalized.
+- `scripts/check_anomaly.py` always returns exit 0 (informational only),
  so it cannot block the DAG even when the design calls for it. Item
  #10 proposes a `--block` flag, but the current code would need to be
  wired up first.
+- Validation error string names differ between batch and streaming (see
  §9.1). The roadmap does not address this.

---

## 11. Glossary

| Term | Meaning |
|---|---|
| **silver** | The "trusted" layer where every row has passed the 10 (or 11) validation rules. Partitioned by `pickup_year/pickup_month`. |
| **quarantine** | The "rejected" layer with the same schema as silver plus `validation_errors ARRAY<VARCHAR>` and `quarantine_ts`. Unpartitioned. |
| **gold** | The "presentation" layer. 33 Parquet datasets produced by `export_gold_to_minio.py` from Trino views. |
| **mart** | The dbt intermediate aggregation layer (8 views). |
| **staging** | The dbt 1:1 cast-and-rename layer over the raw external tables (3 views). |
| **validators** | The 10 business rules in `spark_local_batch.py` and the parallel set in `spark_stream_taxi_events.py`. |
| **trip_id** | `xxhash64(concat_ws('|', pickup_ts, pickup_location_id, dropoff_location_id))` — same definition in batch and streaming so the two sources can be unioned. |

---

## 12. License & data attribution

- Pipeline code: see project root for license.
- Data source: [NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- Test dataset bundled in this repository: 3 months of 2024 data
  (Jan / Feb / Mar), ~8.4M valid + 1.07M invalid rows.
