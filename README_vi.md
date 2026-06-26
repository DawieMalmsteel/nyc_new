# Pipeline Dữ Liệu Taxi NYC

> **Ngôn ngữ**: [Tiếng Việt](README_vi.md) (file này) · [English](README.md)
>
> Nền tảng kỹ thuật dữ liệu đầu-cuối: ingest batch + streaming + CDC từ bản ghi
> Yellow Taxi NYC TLC (Parquet) và PostgreSQL CDC, biến đổi qua Spark → Trino →
> dbt → Superset, điều phối bằng Airflow trên Kubernetes (kind + Skaffold + Helm).

---

Pipeline ingest bản ghi chuyến đi Yellow Taxi NYC từ hai nguồn, validate và
enrich, rồi cung cấp kết quả cho ba nhóm đối tượng:

| Đối tượng | Sản phẩm | Cách dùng |
|---|---|---|
| **Data engineers** | 33 gold dataset Parquet ở `s3a://nyc-gold/` | Xây pipeline mới, train model, audit, export |
| **BI / marketing / sales / CEO** | 30 chart Superset + dashboard trên Postgres Analytics | Quyết định vận hành và chiến lược |
| **Data engineers / SRE** | Trino views, dbt marts, Airflow DAGs | Ad-hoc SQL, dbt lineage, debug pipeline |

### Hai đường ingest

- **Batch** (`jobs/spark_local_batch.py`) — backfill các tháng lịch sử từ
  Parquet ở `s3a://nyc-raw/yellow_taxi/year=YYYY/month=MM/`. Hỗ trợ
  `--incremental` để chỉ xử lý các partition mới hơn partition đã có trong
  silver.
- **Streaming** (`jobs/spark_stream_taxi_events.py`) — consume
  `taxi.trip.events` từ Kafka qua `foreachBatch`, áp cùng logic enrich +
  validate như batch, append vào cùng đường dẫn silver.

### Đường CDC (tuỳ chọn)

`Postgres 16 (WAL logical)` → `Debezium 2.5` → Kafka topic
`nyc_cdc.public.trips` → `scripts/cdc_bridge.py` → Kafka topic
`taxi.trip.events` → Spark Streaming → silver. Ba task DAG độc lập
(`cdc_seed` → `cdc_register` → `cdc_bridge`) seed bảng Postgres từ file
Parquet, đăng ký connector Debezium, và bridge sự kiện CDC về format
taxi chuẩn.

### Sản phẩm đầu ra

- **Valid trips** → `s3a://nyc-silver/trips/` partition theo
  `pickup_year/pickup_month` (dataset hiện tại: ~8.4M dòng từ tháng 1–3/2024).
- **Invalid trips** → `s3a://nyc-quarantine/invalid_trips/` (không partition,
  có `validation_errors ARRAY<VARCHAR>` và `quarantine_ts`).
- **dbt views** → `hive.nyc.*` (raw external), `hive.mart.*` (dbt marts),
  expose qua Trino bằng Hive connector.
- **Gold Parquet** → `s3a://nyc-gold/<dataset>/` — 33 dataset từ
  `scripts/export_gold_to_minio.py` (CTAS).
- **Postgres analytics** → `nyc_analytics.public.*` — atomic swap từ
  `scripts/materialize_to_postgres.py`.
- **Superset** → 30 chart definitions + dashboard trên `nyc_analytics.public.*`.

---

## 2. Kiến trúc

### 2.1 Sơ đồ thành phần

```mermaid
flowchart TD
    subgraph SOURCE["1. Nguồn"]
        RAW["📦 MinIO nyc-raw<br/>Parquet"]
        PGC["🐘 Postgres CDC<br/>nyc_taxi.trips"]
    end

    subgraph INGEST["2. Ingest (Spark)"]
        SB["⚡ spark_batch<br/>local[*]"]
        SS["⚡ spark_streaming<br/>Kafka foreachBatch"]
    end

    subgraph CDC["3. Chuỗi CDC (tuỳ chọn)"]
        DZ["🔗 Debezium 2.5"]
        KFK["📨 Kafka<br/>taxi.trip.events"]
        BR["🌉 cdc_bridge"]
    end

    subgraph STORAGE["4. MinIO S3"]
        SIL["✅ nyc-silver/trips<br/>partitioned"]
        QUA["⚠️ nyc-quarantine"]
        LKP["📋 nyc-lookup"]
        GLD["📦 nyc-gold/*<br/>33 dataset"]
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
        SUP["📈 Superset<br/>30 chart"]
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

| Tầng | Công nghệ | Phiên bản |
|---|---|---|
| Lưu trữ | MinIO S3 | (latest) |
| Xử lý batch | Apache Spark | 3.5.1 |
| Xử lý stream | Apache Spark Structured Streaming | 3.5.1 |
| Nhắn tin | Confluent Kafka + ZooKeeper | 7.6.1 |
| CDC | Debezium Kafka Connect | 2.5 |
| SQL catalog | Trino (Hive connector, file-based metastore) | 435 |
| Biến đổi | dbt-trino | ≥1.7, <2.0 |
| Trực quan hoá | Apache Superset | 4.0.0 |
| Điều phối | Apache Airflow (KubernetesPodOperator) | 2.10.5 |
| Analytics store | PostgreSQL 16 | 16-alpine |
| Container / deploy | kind + Skaffold + Helm | kind ≥0.20, Skaffold v2, Helm ≥3 |
| Alt: dev local | Docker Compose | (16 services) |

### 2.3 Luồng dữ liệu tổng quan

```mermaid
flowchart TD
    subgraph SRC["Nguồn"]
        RAW["📦 MinIO nyc-raw<br/>Parquet"]
        KAFKA[("📨 Kafka<br/>taxi.trip.events")]
    end

    subgraph CDC["Chuỗi CDC (tuỳ chọn)"]
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
        SUP["📈 Superset<br/>30 chart + dashboard"]
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

## 3. Cấu trúc repo

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

    subgraph ROOT["File root"]
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

## 4. Thành phần chi tiết

```mermaid
flowchart LR
    subgraph SRC["Nguồn"]
        RAW["📦 nyc-raw<br/>Parquet"]
        LKP["📋 nyc-lookup<br/>taxi_zone_lookup.csv"]
        PG["🐘 Postgres CDC<br/>(tuỳ chọn)"]
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

    subgraph SCRIPTS2["Export scripts/"]
        GE["export_gold_to_minio.py<br/>33 dataset CTAS<br/>→ s3://nyc-gold/"]
        MP["materialize_to_postgres.py<br/>atomic swap"]
    end

    subgraph SERVE["Serve"]
        PGDB[("Postgres<br/>nyc_analytics.public")]
        SUP["📈 Superset<br/>46 dataset + 30 chart"]
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
        HC{"MinIO<br/>/health/live<br/>timeout 120s"}
    end

    subgraph INCR["Tuỳ chọn: --incremental"]
        MAX["Đọc max(pickup_year),<br/>max(pickup_month)<br/>từ silver hiện tại"]
        FILTER["Lọc input chỉ<br/>giữ partition mới hơn"]
        SKIP{"Có dữ liệu<br/>mới?"}
    end

    subgraph TRANSFORM["Biến đổi"]
        CAST["Cast kiểu<br/>VendorID→int, timestamps, …"]
        ZONE["Zone cleaning<br/>Unknown / N/A / NV → NULL"]
        ENRICH["Enrich cột<br/>trip_id (xxhash64)<br/>event_ts, ingestion_ts<br/>pickup_year/month/date/hour"]
        FILE_FILTER["input_file_name() filter<br/>loại edge rows từ<br/>các tháng liền kề"]
    end

    subgraph ZONE_JOIN["Zone lookup join"]
        PZ["pickup_zones<br/>(đổi tên cột)"]
        DZ["dropoff_zones<br/>(đổi tên cột)"]
        JOIN["left join × 2"]
    end

    subgraph VALIDATE["10 validation rule<br/>(xem §4.5)"]
        ERR["error_array<br/>gom tag lỗi"]
        CHECK["is_valid = size(errors) == 0"]
    end

    subgraph OUTPUT["Output (mode=append)"]
        SIL["✅ s3a://nyc-silver/trips<br/>partition theo<br/>pickup_year/pickup_month"]
        QUA["⚠️ s3a://nyc-quarantine/invalid_trips<br/>+ validation_errors<br/>+ quarantine_ts"]
    end

    RAW --> HC
    LKP --> HC
    HC -->|ready| RAW
    HC -->|ready| LKP

    RAW --> INCR
    MAX --> FILTER
    FILTER --> SKIP
    SKIP -->|Không| DONE["Trả về sớm<br/>không có dữ liệu mới"]
    SKIP -->|Có| CAST

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

- **Master**: `local[*]` (một pod, chạy cả driver và executor).
- **Input**: `s3a://nyc-raw/yellow_taxi/year=*/month=*/*.parquet`.
- **Lookups**: `s3a://nyc-lookup/taxi_zone_lookup.csv` (265 dòng).
- **Output**: `s3a://nyc-silver/trips` (valid, partition theo
  `pickup_year/pickup_month`, `mode("append")`) và
  `s3a://nyc-quarantine/invalid_trips` (không partition).
- **CLI args**: `--input`, `--lookup`, `--silver`, `--quarantine`, `--year`,
  `--month`, `--incremental`.
- **S3A packages**: `org.apache.hadoop:hadoop-aws:3.3.4` +
  `com.amazonaws:aws-java-sdk-bundle:1.12.262` qua `--packages`.
- **MinIO health probe** trước khi đọc (`/minio/health/live`, 120s).
- **Chế độ Incremental**: đọc `max(pickup_year), max(pickup_month)` từ đường
  dẫn silver hiện tại, lọc input chỉ giữ partition mới hơn. Trả về sớm nếu
  không có gì mới.
- **Year/month filter** qua `input_file_name()` — loại edge rows thuộc tháng
  liền kề ngay cả khi file được đặt tên cho tháng mục tiêu.
- **Zone cleaning**: `Borough.isin("Unknown","N/A","NV")` → `NULL` (xử lý
  NYC TLC zone ID 264 và 265).
- **Enriched columns**: `trip_id` (xxhash64 của
  `pickup_ts|pickup_loc|dropoff_loc`), `event_ts`, `ingestion_ts`,
  `pickup_date`, `pickup_hour`, `pickup_year`, `pickup_month`, cộng với
  6 cột zone × 2 (pickup + dropoff).
- **10 validation rule** (xem §4.5).

### 4.2 Spark streaming — `jobs/spark_stream_taxi_events.py`

```mermaid
flowchart TD
    subgraph KAFKA["Kafka source"]
        T["📨 taxi.trip.events<br/>subscribe=topic<br/>startingOffsets=earliest<br/>failOnDataLoss=false"]
    end

    subgraph READ["ReadStream"]
        RAW["spark.readStream<br/>.format(kafka)"]
        PARSE["from_json(value, EVENT_SCHEMA)<br/>20 cột"]
    end

    subgraph TRANSFORM["Biến đổi"]
        TS["to_timestamp<br/>pickup_ts · dropoff_ts · event_ts"]
        IDS["ingestion_ts · pickup_date<br/>pickup_hour · pickup_year<br/>pickup_month"]
        TRIPID["trip_id<br/>xxhash64(pickup_ts|pickup_loc|dropoff_loc)"]
    end

    subgraph ZONE["Zone join"]
        PZ["pickup_zones<br/>(4 cột)"]
        DZ["dropoff_zones<br/>(4 cột)"]
        JOIN["left join × 2"]
    end

    subgraph VALIDATE["11 validation rule<br/>(10 từ batch + event_id_null)"]
        ERR["error_array<br/>gom tag"]
        IS["is_valid = size(errors) == 0"]
    end

    subgraph FB["Vòng foreachBatch"]
        PERSIST["batch_df.persist()"]
        SPLIT["lọc is_valid == true / false"]
        WRITEV["valid → silver<br/>partitionBy(year, month)"]
        WRITEQ["invalid → quarantine<br/>(không partition)"]
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

- **Master**: `local[*]` (one-shot qua `--trigger-available-now`, nếu không
  thì chạy liên tục).
- **Kafka**: `subscribe=taxi.trip.events`,
  `startingOffsets=earliest`, `failOnDataLoss=false`.
- **Schema**: 20 cột khai báo trong `EVENT_SCHEMA` (vendor_id,
  pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
  payment_type, …).
- **Đường dẫn output**: giống batch (`s3a://nyc-silver/trips` và
  `s3a://nyc-quarantine/invalid_trips`).
- **Checkpoint**: `s3a://nyc-silver/checkpoints/spark_stream_taxi_events/taxi.trip.events`.
- **`foreachBatch`**: persist batch DataFrame, tách valid/invalid, ghi
  từng phần, unpersist.
- **11 validation rule**: 10 từ batch + `event_id_null` (chỉ streaming).
- **CLI args**: `--bootstrap-server`, `--topic`, `--lookup-path`,
  `--silver-path`, `--quarantine-path`, `--checkpoint-path`,
  `--trigger-available-now`.

### 4.3 CDC scripts

```mermaid
flowchart LR
    subgraph SEED["cdc_seed.py — seed một lần"]
        P_IN["📦 File Parquet<br/>--input"]
        PANDAS["Pandas đọc<br/>+ đổi tên cột<br/>+ dropna + sample(max_rows)"]
        TRUNC["TRUNCATE trips<br/>RESTART IDENTITY CASCADE"]
        INSERT["SQLAlchemy to_sql<br/>method='multi'"]
        PG_OUT[("🐘 Postgres<br/>nyc_taxi.trips")]
        P_IN --> PANDAS --> TRUNC --> INSERT --> PG_OUT
    end

    subgraph REG["cdc_register_connector.py — idempotent"]
        WAIT_DZ["chờ Debezium REST<br/>/connectors (timeout 60s)"]
        DEL["DELETE nếu tồn tại<br/>(idempotency)"]
        POST["POST /connectors<br/>name: nyc-postgres-connector"]
        CFG["Config:<br/>plugin.name=pgoutput<br/>transforms=unwrap<br/>ExtractNewRecordState<br/>snapshot.mode=never<br/>poll.interval.ms=500"]
        DZ_SVC[("🔗 Debezium<br/>svc-debezium:8083")]
        WAIT_DZ --> DEL --> POST --> DZ_SVC
        CFG -.-> POST
    end

    subgraph BRG["cdc_bridge.py — CDC → format chuẩn"]
        CONS["KafkaConsumer<br/>input-topic: nyc_cdc.public.trips<br/>group_id: cdc-bridge-{uuid}<br/>enable_auto_commit=false"]
        TRANS["transform(event)<br/>unwrap → flat event<br/>thêm event_id, source_file, …"]
        PROD["KafkaProducer<br/>output-topic: taxi.trip.events<br/>linger_ms=100, batch_size=64KB<br/>compression=gzip<br/>flush mỗi 500 event"]
        KFK[("📨 Kafka<br/>taxi.trip.events")]
        CONS --> TRANS --> PROD --> KFK
    end

    PG_OUT -->|WAL logical| DZ_SVC
    DZ_SVC -->|Debezium envelope| CONS
```

- `scripts/cdc_seed.py` — đọc file Parquet bằng Pandas, đổi tên cột,
  TRUNCATE `nyc_taxi.trips`, insert qua `to_sql(method='multi')`. Mặc
  định `--max-rows 5000`, sample `random_state=42`.
- `scripts/cdc_register_connector.py` — POST config Debezium connector
  lên `${debezium-url}/connectors`. Idempotent (DELETE rồi POST). Cấu
  hình chính: `plugin.name=pgoutput`, `transforms=unwrap` với
  `ExtractNewRecordState`, `snapshot.mode=never`,
  `transforms.unwrap.drop.tombstones=false`, `poll.interval.ms=500`.
- `scripts/cdc_bridge.py` — consume topic Debezium, transform mỗi event
  về format NYC Taxi chuẩn (cùng field với streaming schema), produce sang
  `taxi.trip.events`. Hiệu năng: `linger_ms=100`, `batch_size=65536`,
  `compression_type=gzip`. Async send với flush định kỳ mỗi
  `--flush-interval` event. **Group id sinh mỗi run**
  (`cdc-bridge-{uuid4.hex[:8]}`), nên mỗi lần chạy đọc từ offset sớm nhất
  — chấp nhận được cho one-shot DAG nhưng gây trùng event nếu bridge bị
  restart trong khi topic còn data chưa consume. Exit code: `0` khi tắt
  graceful, `1` khi lỗi kết nối Kafka.

### 4.4 Trino catalog — `scripts/trino_register.py`

```mermaid
flowchart TD
    WAIT["wait_for_trino()<br/>SELECT 1 polling<br/>timeout 300s"]
    SCHEMA["CREATE SCHEMA IF NOT EXISTS hive.nyc"]
    T1["DROP TABLE trips<br/>CREATE TABLE trips<br/>Parquet, partition theo<br/>pickup_year/pickup_month<br/>29 cột"]
    T2["DROP TABLE invalid_trips<br/>CREATE TABLE invalid_trips<br/>Parquet, không partition<br/>+ validation_errors ARRAY(VARCHAR)<br/>+ quarantine_ts"]
    T3["DROP TABLE taxi_zone_lookup<br/>CREATE TABLE taxi_zone_lookup<br/>CSV có header, 4 cột"]
    SYNC["CALL hive.system.sync_partition_metadata<br/>(schema_name='nyc',<br/>table_name='trips', mode='FULL')"]
    SMOKE["SELECT COUNT(*) × 3<br/>(trips, invalid_trips, taxi_zone_lookup)"]

    S1["s3a://nyc-silver/trips<br/>(env SILVER_PATH)"]
    S2["s3a://nyc-quarantine/invalid_trips<br/>(env QUARANTINE_PATH)"]
    S3["s3a://nyc-lookup/<br/>(env ZONES_PATH)"]

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

- Idempotent: `DROP TABLE IF EXISTS` + `CREATE TABLE` cho 3 bảng.
- `hive.nyc.trips` (Parquet, partition theo `pickup_year/pickup_month`).
- `hive.nyc.invalid_trips` (Parquet, không partition, có
  `validation_errors ARRAY(VARCHAR)` và `quarantine_ts`).
- `hive.nyc.taxi_zone_lookup` (CSV có header).
- `CALL hive.system.sync_partition_metadata(schema_name => 'nyc',
  table_name => 'trips', mode => 'FULL')` sau khi tạo.
- Smoke test: `SELECT COUNT(*)` cho cả 3 bảng.
- Đường dẫn mặc định đọc từ env `SILVER_PATH`, `QUARANTINE_PATH`,
  `ZONES_PATH` (override được cho S3 vs local FS).

### 4.5 Validation rule

10 rule áp dụng trong batch; streaming thêm rule thứ 11 cho `event_id`.
Mỗi rule sinh ra một chuỗi tag trong mảng `validation_errors`; những dòng
có mảng rỗng được ghi vào silver, phần còn lại vào quarantine.

| # | Rule (chuỗi lỗi batch) | Rule (chuỗi lỗi streaming) |
|---|---|---|
| 1 | `pickup_datetime_null_or_invalid` | giống |
| 2 | `dropoff_datetime_null_or_invalid` | giống |
| 3 | `invalid_trip_duration` | giống |
| 4 | `non_positive_trip_distance` | `trip_distance_must_be_gt_0` |
| 5 | `negative_fare_amount` | `fare_amount_must_be_gte_0` |
| 6 | `total_amount_less_than_fare` | `total_amount_must_be_gte_fare_amount` |
| 7 | `invalid_passenger_count` | `passenger_count_out_of_range` |
| 8 | `payment_type_out_of_range` | giống |
| 9 | `unknown_pickup_location` | `pickup_location_not_found` |
| 10 | `unknown_dropoff_location` | `dropoff_location_not_found` |
| 11 | (không có) | `event_id_null` (chỉ streaming) |

Tầng dbt staging chạy thêm `nullif(nullif(...))` trên cột zone
(`Unknown`, `N/A`, `NV`) như lớp dọn dẹp dự phòng.

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
        T_YML["21 generic yml<br/>not_null + accepted_values<br/>trong 3 file yml"]
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

- 30 SQL model:
  - **Staging** (3) — `stg_trips`, `stg_zones`, `stg_invalid_trips`. Tất cả
    `materialized='view'`. Cast kiểu và dọn chuỗi zone null-ish.
  - **Marts** (8) — `fact_trips`, `fact_invalid_trips`, `dim_zone`,
    `mart_hourly_summary`, `mart_revenue_by_day`, `mart_revenue_by_zone`,
    `mart_payment_type_summary`, `mart_trips_by_hour`. Tất cả
    `materialized='view'`.
  - **Gold** (19) — `gold_fact_trips`, `gold_dim_zone`,
    `gold_mart_revenue_by_day`, `gold_mart_revenue_by_zone`,
    `gold_dq_row_count_trend`, `gold_validation_summary`, cộng 13 model BI
    (`gold_customer_segments`, `gold_customer_journey`,
    `gold_payment_behavior`, `gold_tipping_culture`,
    `gold_trip_unit_economics`, `gold_zone_demand_heatmap`,
    `gold_vendor_battlecard`, `gold_growth_metrics`,
    `gold_executive_daily`, `gold_executive_weekly`,
    `gold_revenue_waterfall`, `gold_hourly_pulse`, `gold_risk_dashboard`).
    Tất cả `materialized='view'` trong dev.
- **27 test**:
  - 6 singular SQL: `assert_minimum_rows`, `assert_recent_data`,
    `passenger_count_range`, `payment_type_range`,
    `total_not_less_than_fare`, `trip_distance_positive`.
  - 21 generic trong file yml (`fact_invalid_trips_tests.yml`,
    `fact_trips_tests.yml`, `stg_trips_tests.yml`) — kiểm tra `not_null`
    và `accepted_values`.
- `profiles.yml` — target `dev` trỏ vào Trino (`type: trino`,
  `host: svc-trino`, `port: 8080`, `database: hive`, `schema: mart`,
  `threads: 4`).
- `dbt build` chạy models + test; kỳ vọng `30 models, 27 tests`.

### 4.7 Gold export — `scripts/export_gold_to_minio.py`

```mermaid
flowchart TD
    WAIT["wait_for_trino()<br/>timeout 120s"]
    SCHEMA["CREATE SCHEMA IF NOT EXISTS<br/>hive.nyc_gold"]
    LOOP{{"Với mỗi dataset trong<br/>GOLD_DATASETS (33)"}}
    CT["SELECT COUNT(*) smoke test"]
    DROP["DROP TABLE IF EXISTS<br/>hive.nyc_gold.<name>"]
    CLEAN["clean_s3_path(bucket, prefix)<br/>xóa đệ quy qua minio client"]
    CTAS["CREATE TABLE hive.nyc_gold.<name><br/>WITH (external_location=<br/>'s3://nyc-gold/<name>/',<br/>format='PARQUET')<br/>AS <sql>"]
    RENAME["_add_parquet_extensions()<br/>đổi tên data file thành *.parquet"]
    NEXT["dataset kế tiếp"]
    GOLD_OUT[("📦 s3://nyc-gold/<br/>33 dataset, Parquet")]
    EXIT{"Cả 33<br/>thành công?"}
    OUT0["exit 0"]
    OUT1["exit 1<br/>(DAG task fail)"]

    WAIT --> SCHEMA --> LOOP
    LOOP --> CT --> DROP --> CLEAN --> CTAS --> RENAME --> NEXT
    NEXT --> LOOP
    LOOP -->|xong| EXIT
    EXIT -->|Có| OUT0
    EXIT -->|Không| OUT1
    CTAS -.-> GOLD_OUT
```

- 33 dataset định nghĩa trong list `GOLD_DATASETS`. Mỗi entry:
  `{name, sql, location_subdir}`.
- Với mỗi dataset: smoke test `SELECT COUNT(*)` → `DROP TABLE IF EXISTS
  hive.nyc_gold.<name>` → `clean_s3_path` (xóa đệ quy các object cũ trong
  prefix) → `CREATE TABLE … WITH (external_location=…) AS <sql>`.
- Đổi tên file Parquet thành đuôi `.parquet` (đôi khi Hive ghi ra không
  có đuôi tuỳ layout partition).
- Trả exit 0 nếu tất cả thành công, 1 nếu có dataset lỗi.

### 4.8 Materialize to Postgres — `scripts/materialize_to_postgres.py`

```mermaid
flowchart TD
    WAIT_PG["wait_for_postgres()<br/>psycopg2 connect, 120s"]
    WAIT_TR["wait_for_trino()<br/>300s"]
    IMP["from export_gold_to_minio import GOLD_DATASETS<br/>(single source of truth — 33 SQL)"]
    LOOP{{"Với mỗi dataset (33)"}}
    RUN_SQL["trino_cur.execute(sql)<br/>fetchall()"]
    MAP["TYPE_MAP trino → postgres<br/>(varchar→TEXT, double→DOUBLE PRECISION, …)"]
    CR["CREATE TABLE<br/><name>_new (col_defs)"]
    NAN["safe_rows:<br/>NaN → None"]
    INS["execute_values(<br/>page_size=5000)"]
    SWAP["DROP TABLE IF EXISTS <name><br/>ALTER TABLE <name>_new RENAME TO <name><br/>(atomic)"]
    NEXT["dataset kế tiếp"]
    PG[("🐘 Postgres analytics<br/>nyc_analytics.public.*<br/>33 bảng")]
    EXIT{"Cả 33<br/>thành công?"}
    OUT0["exit 0"]
    OUT1["exit 1"]

    WAIT_PG --> IMP
    WAIT_TR --> IMP
    IMP --> LOOP
    LOOP --> RUN_SQL --> MAP --> CR --> NAN --> INS --> SWAP --> NEXT
    NEXT --> LOOP
    SWAP -.-> PG
    LOOP -->|xong| EXIT
    EXIT -->|Có| OUT0
    EXIT -->|Không| OUT1
```

- Import `GOLD_DATASETS` từ `export_gold_to_minio.py` để SQL là single
  source of truth.
- Với mỗi dataset: chạy cùng SQL đó trên Trino, đọc kết quả, map kiểu
  Trino → Postgres (`TYPE_MAP`), tạo `<name>_new`, batch insert qua
  `execute_values(page_size=5000)`, rồi atomic-swap:
  `DROP TABLE IF EXISTS <name>; ALTER TABLE <name>_new RENAME TO <name>`.
- Làm sạch `NaN` → `None` trước khi insert.
- Trả exit 0 nếu tất cả thành công, 1 nếu dataset nào lỗi.

### 4.9 Superset — `scripts/superset_bootstrap.py` và
`scripts/superset_saved_queries.py`

```mermaid
flowchart TD
    subgraph BOOT["superset_bootstrap.py — idempotent"]
        LOGIN["POST /security/login<br/>lấy access_token"]
        DB["1 database<br/>'NYC Trino' → postgresql://<br/>svc-postgres-analytics"]
        DS["46 dataset<br/>(list GOLD_TABLES:<br/>33 base + 13 BI)"]
        CH["30 chart (CHART_DEFS)<br/>bar / line / pie / table<br/>big_number_total / dist_bar<br/>echarts_timeseries_bar/line"]
        DASH["1 dashboard<br/>'NYC Taxi Overview'<br/>slug=nyc-taxi"]
        LOGIN --> DB --> DS --> CH --> DASH
    end

    subgraph SQL["superset_saved_queries.py — idempotent"]
        SQL_LOGIN["POST /security/login"]
        LIST["GET /saved_query/"]
        CREATE["POST /saved_query/<br/>cho mỗi entry chưa có"]
        SQL_LOGIN --> LIST --> CREATE
    end

    subgraph TARGET["Đích"]
        T_PG[("🐘 Postgres<br/>nyc_analytics.public.*<br/>cho chart/dashboard")]
        T_TR[("🔍 Trino<br/>hive.mart / hive.nyc_gold<br/>cho saved queries")]
    end

    DB -.-> T_PG
    DS -.-> T_PG
    CH -.-> T_PG
    DASH -.-> T_PG
    CREATE -.-> T_TR

    BOOT -.->|"config: WTF_CSRF_ENABLED=False<br/>TALISMAN_ENABLED=False"| BOOT
```

- `superset_bootstrap.py` đăng ký (idempotent):
  - 1 database (`NYC Trino` → `postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics`).
  - 46 dataset trong list `GOLD_TABLES` (33 base + 13 BI).
  - 30 chart definition trong `CHART_DEFS` (bar / line / pie / table /
    big-number / echarts timeseries / dist bar).
  - 1 dashboard `nyc-taxi`.
  - Dùng `WTF_CSRF_ENABLED = False` và `TALISMAN_ENABLED = False` cho
    các POST call bootstrap.
- `superset_saved_queries.py` đăng ký 25 saved query trong SQL Lab, trỏ
  vào schema `hive.mart` và `hive.nyc_gold` của Trino.

### 4.10 Anomaly check — `scripts/check_anomaly.py`

- Đọc `hive.mart.gold_dq_row_count_trend` và đánh dấu những dòng có
  `anomaly_flag != 'NORMAL'`.
- In các anomaly và summary (`low`, `high`, `total_days`).
- **Exit code 0 luôn** — chỉ thông tin, không block DAG.

---

## 5. Airflow DAGs

Cả ba DAG dùng `KubernetesPodOperator` (KPO) với các default chung:

- `namespace="nyc-taxi"`, `in_cluster=True`, `service_account_name="airflow-sa"`
- `get_logs=True` (stream pod log vào Airflow task log)
- `volumes=[project_volume]` mount `project-files-pvc` vào `/opt/project`
  (chứa `airflow/dags/`, `jobs/`, `scripts/`, `dbt/`, `charts/`)
- `image_pull_policy="IfNotPresent"` (dùng image đã load vào kind local,
  không pull registry)
- `security_context=run_as_user=0` cho task Spark (để S3A filesystem có
  thể ghi vào PVC)

### 5.0 Cấu hình task chung

| Thiết lập | `nyc_e2e_pipeline` | `nyc_analytics_refresh` | `nyc_cdc_pipeline` |
|---|---|---|---|
| `retries` | 3 | 3 | 2 |
| `retry_delay` | 30s | 30s | 30s |
| `execution_timeout` | 30 min | 30 min | 15 min |
| `depends_on_past` | `False` | `False` | `False` |
| `max_active_runs` | 1 | 1 | 1 |
| `catchup` | (default) | `False` | `False` |
| Schedule | `None` (manual) | `@weekly` | `None` (manual) |
| `start_date` | 2024-01-01 | 2026-01-01 | 2024-01-01 |

### 5.1 `nyc_e2e_pipeline` — 13 task, trigger thủ công

Toàn bộ pipeline E2E: chuỗi CDC tuỳ chọn (seed → register → bridge) đẩy
sự kiện vào đường streaming; batch và stream đều hội tụ tại Trino; dbt
build view; gold + materialize chạy song song; Superset bootstrap lại;
bộ câu hỏi analytics đóng run.

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

**Trigger rule**: `trino_bootstrap` dùng `trigger_rule="one_success"` — chạy
nếu `spark_batch` HOẶC `spark_streaming` thành công. Cho phép chạy chỉ
batch, chỉ CDC, hoặc cả hai mà không phải sửa DAG. Phần còn lại dùng
default `all_success`.

**Giữ pod**: `trino_bootstrap` và `cdc_seed` set
`is_delete_operator_pod=False` để pod log còn lại sau khi task xong (hữu
ích debug S3A / Trino bootstrap fail). 11 task còn lại dùng default `True`
và tự cleanup.

**`trino_bootstrap` startup**: `startup_timeout_seconds=600` — Trino có
thể cần khá lâu để ready lần đầu sau Helm deploy mới.

#### Chi tiết task

| Task | Image | Command / Args | Volume (thêm) | Env var (thêm) | Ghi chú |
|---|---|---|---|---|---|
| `cdc_seed` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-seed --input /opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --max-rows 1000 --dsn postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi` | `raw-data-pvc` → `/mnt/nyc-data` | — | `is_delete_operator_pod=False` |
| `cdc_register` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-register --debezium-url http://svc-debezium:8083 --postgres-host svc-postgres-cdc` | — | — | Idempotent (DELETE + POST) |
| `cdc_bridge` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-bridge --bootstrap-server svc-kafka:9092 --input-topic nyc_cdc.public.trips --output-topic taxi.trip.events --idle-timeout 30 --flush-interval 500` | — | — | Tự thoát sau 30s không có message; in benchmark khi kết thúc |
| `spark_batch` | `apache/spark:3.5.1` | `spark-submit --master local[*] --packages hadoop-aws:3.3.4,aws-java-sdk-bundle:1.12.262 --conf spark.jars.ivy=/opt/project/.ivy2 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.scheduler.mode=FAIR /opt/project/jobs/spark_local_batch.py --input s3a://nyc-raw/yellow_taxi/year=*/month=*/*.parquet --lookup s3a://nyc-lookup/taxi_zone_lookup.csv --silver s3a://nyc-silver/trips --quarantine s3a://nyc-quarantine/invalid_trips --incremental` | — | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | `security_context=run_as_user=0` |
| `spark_streaming` | `apache/spark:3.5.1` | `spark-submit --master local[*] --conf spark.jars.ivy=/opt/project/.ivy2 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.scheduler.mode=FAIR --packages spark-sql-kafka-0-10_2.12:3.5.1,hadoop-aws:3.3.4,aws-java-sdk-bundle:1.12.262 /opt/project/jobs/spark_stream_taxi_events.py --bootstrap-server svc-kafka:9092 --topic taxi.trip.events --lookup-path s3a://nyc-lookup/taxi_zone_lookup.csv --silver-path s3a://nyc-silver/trips --quarantine-path s3a://nyc-quarantine/invalid_trips --checkpoint-path s3a://nyc-silver/checkpoints/spark_stream_taxi_events/taxi.trip.events --trigger-available-now` | — | giống `spark_batch` | `security_context=run_as_user=0`; one-shot qua `--trigger-available-now` |
| `trino_bootstrap` | `nyc-pipeline-tools:latest` | `entrypoint-trino-bootstrap` | — | `TRINO_HOST=svc-trino`, `TRINO_PORT=8080`, `TRINO_USE_SSL=false`, `S3_MODE=true`, `AWS_ACCESS_KEY_ID=minio`, `AWS_SECRET_ACCESS_KEY=minio123`, `AWS_ENDPOINT_URL=http://svc-minio:9000`, `SILVER_PATH=s3://nyc-silver/trips`, `QUARANTINE_PATH=s3://nyc-quarantine/invalid_trips`, `ZONES_PATH=s3://nyc-lookup/` | `is_delete_operator_pod=False`; `startup_timeout_seconds=600`; `trigger_rule="one_success"` |
| `dbt_build` | `nyc-dbt:latest` | `entrypoint-dbt` | — | `DBT_PROFILES_DIR=/opt/project/dbt`, `TRINO_HOST=svc-trino` | Chạy `trino_sync_partitions.py` trước, rồi `dbt build` (30 model + 27 test) |
| `gold_export` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/export_gold_to_minio.py` | — | `TRINO_HOST=svc-trino`, `TRINO_PORT=8080` | 33 dataset qua CTAS sang s3://nyc-gold/ |
| `materialize_postgres` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/materialize_to_postgres.py` | — | `TRINO_HOST`, `TRINO_PORT`, `PG_ANALYTICS_HOST=svc-postgres-analytics`, `PG_ANALYTICS_USER=analytics`, `PG_ANALYTICS_PASSWORD=analytics`, `PG_ANALYTICS_DB=nyc_analytics` | Atomic swap cho 33 bảng |
| `superset_bootstrap` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_bootstrap.py` | — | `SUPERSET_URL=http://svc-superset:8088`, `PG_ANALYTICS_URI=postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics` | 1 DB + 46 dataset + 30 chart + 1 dashboard |
| `superset_saved_queries` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_saved_queries.py` | — | `SUPERSET_URL=http://svc-superset:8088` | 25 saved query trỏ vào Trino |
| `analytics_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/run_analytics_questions.py` | — | `TRINO_HOST`, `TRINO_PORT` | 10 câu SQL từ `sql/analytics_questions.sql`; mỗi câu phải trả ≥1 dòng |
| `anomaly_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/check_anomaly.py` | — | `TRINO_HOST`, `TRINO_PORT` | Đọc `gold_dq_row_count_trend`; chỉ thông tin, exit 0 luôn |

**Ghi chú về image override**: khi chạy DAG, flag `--incremental` trên
`spark_batch` có nghĩa nó sẽ skip partition nếu `max(pickup_year),
max(pickup_month)` trong silver đã bao phủ file input. Nhờ đó DAG có thể
trigger lại an toàn mà không trùng dòng.

### 5.2 `nyc_analytics_refresh` — 7 task, `@weekly`

Chạy lại tầng analytics với giả định tầng silver đã có data (không có
task Spark — giả định đã chạy qua E2E DAG hoặc thủ công). Tái sử dụng
cùng script và image pattern với E2E DAG.

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

**Ngữ nghĩa fan-out**: `dbt_build` có 3 task downstream:

- `materialize_postgres` — khởi động chuỗi materialize.
- `gold_export` — chạy song song, không có downstream (chỉ ghi
  s3://nyc-gold/).
- `anomaly_check` — chạy song song, không có downstream.

`gold_export` và `anomaly_check` là **đầu cuối nhánh**: chúng không
block nhau và không block chuỗi `materialize_postgres`. Nghĩa là
`gold_export` fail KHÔNG làm `analytics_check` bị skip; DAG chỉ fail
đường analytics nếu `materialize_postgres` hoặc một trong các task
Superset fail.

**Chi tiết task** (tất cả KPO, dùng cùng env var như §5.1 trừ khi
override):

| Task | Image | Command / Args | Ghi chú |
|---|---|---|---|
| `dbt_build` | `nyc-dbt:latest` | `entrypoint-dbt` | Giống §5.1 |
| `materialize_postgres` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/materialize_to_postgres.py` | Giống §5.1 |
| `gold_export` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/export_gold_to_minio.py` | Giống §5.1 |
| `anomaly_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/check_anomaly.py` | Giống §5.1 |
| `superset_bootstrap` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_bootstrap.py` | Giống §5.1 |
| `superset_saved_queries` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/superset_saved_queries.py` | Giống §5.1 |
| `analytics_check` | `nyc-pipeline-tools:latest` | `python3 /opt/project/scripts/run_analytics_questions.py` | Giống §5.1 |

**Giữ pod**: khác §5.1, không task nào trong DAG này set
`is_delete_operator_pod=False`. Log bị xoá khi task xong theo default.

### 5.3 `nyc_cdc_pipeline` — 3 task, thủ công

Dùng để demo chuỗi CDC end-to-end mà không chạy full E2E. Seed Postgres
với 5000 dòng từ file Parquet, đăng ký Debezium connector, sau đó bridge
sự kiện CDC vào topic taxi event chuẩn trong ~30 giây rồi thoát.

```mermaid
flowchart LR
    cdc_seed([cdc_seed]) --> cdc_register([cdc_register]) --> cdc_bridge([cdc_bridge])
```

**Chi tiết task**:

| Task | Image | Command / Args | Ghi chú |
|---|---|---|---|
| `cdc_seed` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-seed --input /opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --max-rows 5000 --dsn postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi` | 5000 dòng sample với `random_state=42` |
| `cdc_register` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-register --debezium-url http://svc-debezium:8083 --postgres-host svc-postgres-cdc` | Idempotent |
| `cdc_bridge` | `nyc-pipeline-tools:latest` | `entrypoint-cdc-bridge --bootstrap-server svc-kafka:9092 --input-topic nyc_cdc.public.trips --output-topic taxi.trip.events --idle-timeout 30 --flush-interval 500` | `--idle-timeout 30` tự thoát sau 30s không có message; không `is_delete_operator_pod=False` |

DAG này **không trigger** task Spark nào. Sau khi `cdc_bridge` thoát,
các event nó produce nằm trong topic `taxi.trip.events`. Để consume
phải chạy `spark_streaming` riêng (qua task trong E2E DAG hoặc
`make spark-streaming`).

### 5.4 Hành vi chung và lưu ý

- Không task nào có `depends_on_past=True`. Có thể clear và re-run một
  task đơn lẻ mà không ảnh hưởng state của task downstream.
- `startup_timeout_seconds=600` chỉ set trên `trino_bootstrap`. Tất cả
  task khác dùng KPO default 120s.
- `is_delete_operator_pod` chỉ là `False` trên `trino_bootstrap` và
  `cdc_seed` (đều trong `nyc_e2e_pipeline`). Khi debug hai task này,
  pod sẽ tồn tại sau khi task xong; các task khác tự cleanup.
- **ServiceAccount**: `airflow-sa` (định nghĩa trong
  `charts/nyc-taxi/templates/airflow/rbac.yaml`). Có quyền
  `get/list/watch` và `create/delete` trên pods và pod logs.
- **Tham chiếu image** giả định image đã được load vào kind cluster
  local (Skaffold pre-deploy hook xử lý việc này). Nếu chạy trên
  cluster remote, đổi `image_pull_policy` thành `Always` và push image
  lên registry.
- **Ngữ nghĩa restart**: task fail sẽ retry 3 lần (2 lần cho CDC) với
  backoff 30s. Sau 3 lần fail, task được đánh failed và DAG run fail;
  các task downstream bị skip.

---

## 6. Triển khai (Kubernetes / Skaffold — chính)

### 6.1 Một lệnh deploy

```bash
# Lần đầu:
bash scripts/cluster_up.sh          # tạo kind cluster + load public images

# Mỗi lần:
skaffold dev --namespace nyc-taxi   # build images, deploy Helm, port-forward, watch
```

Skaffold:

- Build 4 image: `nyc-pipeline-tools`, `nyc-dbt`, `nyc-airflow`,
  `nyc-superset`.
- Pre-deploy hook: xoá immutable job trong namespace, tar-sync
  `airflow/dags/`, `jobs/`, `scripts/`, `dbt/`, `charts/` vào
  `project-files-pvc` hostPath trên node `kind-worker`.
- Helm deploy chart ở `charts/nyc-taxi/` (44 manifest K8s hợp lệ trong
  13 folder thành phần).
- Port-forward 8 dịch vụ sang `localhost:39080-39087`.

### 6.2 Port-forward dịch vụ

| Dịch vụ | URL | Thông tin đăng nhập |
|---|---|---|
| Superset | `http://localhost:39080` | `admin` / `admin` |
| MinIO API | `http://localhost:39081` | `minio` / `minio123` |
| Kafka UI | `http://localhost:39082` | — |
| Spark Master | `http://localhost:39083` | — |
| Trino | `http://localhost:39084` | — |
| Airflow | `http://localhost:39085` | `admin` / `admin` |
| MinIO Console | `http://localhost:39086` | `minio` / `minio123` |
| Postgres CDC | `http://localhost:39087` | `postgres` / `postgres` |

### 6.3 Tên service K8s (bắt buộc prefix `svc-`)

Bên trong cluster, tất cả tên service đều có prefix `svc-`. Code trong
repo dùng trực tiếp các tên này:

- `svc-minio:9000` (S3A + Trino Hive connector)
- `svc-trino:8080` (Trino JDBC)
- `svc-kafka:9092` (Spark streaming)
- `svc-postgres-cdc:5432` (Debezium + cdc_seed)
- `svc-postgres-analytics:5432` (materialize + Superset)
- `svc-debezium:8083` (cdc_register REST API)
- `svc-superset:8088` (Superset API)
- `svc-airflow-webserver:8080` (Airflow web UI)

### 6.4 Tài nguyên K8s do chart tạo

| Kind | Số lượng | Ví dụ |
|---|---|---|
| `Deployment` | 8 | kafka-ui, superset, trino, deb, airflow-webserver, airflow-scheduler, spark-master, spark-worker |
| `StatefulSet` | 4 | zookeeper, kafka, postgres-cdc, postgres-analytics, airflow-postgres |
| `Service` | 12 | svc-* cho mỗi thành phần trên |
| `Job` | 4 | topic-init, postgres-init, minio-setup, airflow-init |
| `ConfigMap` | 3 | airflow entrypoint, superset config, trino config |
| `PersistentVolume` / `PVC` | 5 | project-files-pv, raw-data-pv, minio-data, postgres-cdc, postgres-analytics, airflow-postgres |
| `ServiceAccount` / `Role` / `RoleBinding` | 1 set | airflow-sa |
| `Namespace` | 1 | nyc-taxi |

> Ghi chú: chart source có thêm 3 file template ở
> `charts/nyc-taxi/templates/trino/charts/nyc-taxi/templates/trino/*.yaml`
> mà Helm không load do đường dẫn nested lỗi. Chúng là bản trùng của
> template chính và có thể xoá.

### 6.5 Cấu hình Trino (hiện tại)

- `docker/trino/etc/jvm.config`: `-Xmx2G` (heap 2 GB, G1GC, region 32 MB).
- `docker/trino/etc/config.properties`: single-node coordinator, port 8080.
- `docker/trino/etc/catalog/hive.properties`: file-based metastore ở
  `/data/trino-metastore`, `hive.recursive-directories=true`,
  `hive.s3.endpoint=http://minio:9000` (Compose) hoặc `svc-minio:9000`
  (K8s), `hive.s3.path-style-access=true`, `hive.ssl.enabled=false`,
  `hive.non-managed-table-creates-enabled=true`,
  `hive.non-managed-table-writes-enabled=true`.

### 6.6 Cấu hình Postgres CDC (hiện tại)

- Image: `postgres:16-alpine`.
- Replication args: `wal_level=logical`, `max_replication_slots=4`,
  `max_wal_senders=4`.
- StatefulSet với PVC (không backup, không HA trong dev).
- `POSTGRES_PASSWORD=postgres`, `POSTGRES_USER=postgres`,
  `POSTGRES_DB=nyc_taxi`.

### 6.7 Cấu hình Kafka (hiện tại)

- Image: `confluentinc/cp-kafka` (7.x).
- Single broker, single pod.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`,
  `KAFKA_TRANSACTION_STATE_LOG_*_REPLICATION_FACTOR=1`.
- Topics tạo khi khởi động bằng `scripts/create_kafka_topics.py`:
  `taxi.trip.events` (3 partition), `taxi.trip.invalid` (3 partition),
  `taxi.trip.dlq` (3 partition).

---

## 7. Phát triển local (Docker Compose)

```bash
make infra-up           # start ZK + Kafka + MinIO + Spark
make kafka-topics       # tạo topics
make spark-batch MONTH=03
make trino-bootstrap
make dbt-build
make gold-export
make superset-bootstrap
make verify-all         # E2E 7 bước
make clean-all
```

29 target Makefile tổng. Compose stack có 16 service, 6 profile group
(`default`, `tools`, `trino`, `dbt`, `superset`, `airflow`).

Cổng ở chế độ Compose (khác K8s):

| Dịch vụ | Cổng |
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

Sau một full pipeline run, các check sau được kỳ vọng pass:

| Check | Cách chạy | Kỳ vọng |
|---|---|---|
| `dbt build` | `make dbt-build` hoặc DAG `nyc_analytics_refresh` | 30 model, 27 test, 0 lỗi |
| Mart row counts | `make verify-mart` hoặc `scripts/verify_mart.py` | `dim_zone ≈ 261`, `fact_trips ≈ 8.4M`, `mart_hourly_summary ≈ 11K`, `mart_revenue_by_day ≈ 90` |
| Analytics questions | `make verify-analytics` hoặc `scripts/run_analytics_questions.py` | 10/10 PASS (mỗi query trả ≥1 dòng) |
| CDC pipeline | `make verify-cdc` | Postgres row count > 0, Debezium connector RUNNING |
| Anomaly check | `scripts/check_anomaly.py` (DAG task) | chỉ thông tin, exit 0 |

`run_analytics_questions.py` đọc `sql/analytics_questions.sql`, tách
theo marker title `-- N)`, chạy mỗi câu trên Trino, và assert mỗi câu
trả về ít nhất một dòng. Mỗi câu phải kết thúc bằng `;`.

---

## 9. Giới hạn đã biết

Đây là đặc điểm của code tính đến thời điểm viết README; project là
demo, không phải platform production-ready.

### 9.1 Chuỗi lỗi validation khác nhau giữa batch và streaming

Cùng một rule logic nhưng sinh chuỗi lỗi khác nhau trong
`jobs/spark_local_batch.py` (vd `non_positive_trip_distance`) so với
`jobs/spark_stream_taxi_events.py` (vd `trip_distance_must_be_gt_0`).
5 rule bị lệch. Một cleanup tương lai nên align và quyết định một
chuẩn đặt tên.

### 9.2 Batch và streaming ghi cùng đường dẫn silver

Cả hai job ghi vào `s3a://nyc-silver/trips/`. Không có cột hay metadata
đánh dấu nguồn dòng. Điều này khiến không thể monitor độc lập output
batch và stream, hoặc rollback một phần mà không ảnh hưởng phần còn lại.

### 9.3 CDC bridge sinh consumer group mới mỗi run

`scripts/cdc_bridge.py` dùng
`group_id = f"cdc-bridge-{uuid4().hex[:8]}"`, nên mỗi lần gọi bắt đầu từ
offset sớm nhất. Chấp nhận được cho one-shot DAG (bridge thoát sau
`--idle-timeout` giây), nhưng gây trùng event nếu bridge bị restart
trong khi topic nguồn còn data chưa consume.

### 9.4 Không có quality gate trong main DAG

Không có task `verify_silver` / `verify_gold` / `verify_postgres` /
`verify_superset` / `verify_freshness` trong bất kỳ DAG nào.
`analytics_check` và `anomaly_check` chỉ thông tin (exit 0 luôn).

### 9.5 Trino chạy single node với file-based metastore

- Single coordinator (không HA).
- Metastore: file-based tại `/opt/project/data/trino-metastore` (PVC).
  Nếu PVC mất, catalog phải tạo lại bằng cách chạy lại
  `scripts/trino_register.py`.
- JVM heap: `-Xmx2G` — query CTAS lớn có thể OOM.
- `gold_export` chạy 33 CTAS tuần tự. Bước `clean_s3_path` đã có để
  recover khi fail một phần, nhưng một lần OOM vẫn có thể buộc retry
  toàn bộ task.

### 9.6 Helm chart có duplicate template lồng nhau

`charts/nyc-taxi/templates/trino/charts/nyc-taxi/templates/trino/`
chứa 3 file trùng (configmap, deployment, service) mà Helm không load
do đường dẫn. Có thể xoá; template thật ở
`charts/nyc-taxi/templates/trino/*.yaml`.

### 9.7 Spark streaming dùng `failOnDataLoss=false`

Nếu Kafka retention drop một message giữa hai lần Spark run, việc skip
im lặng che giấu data loss. Production nên chuyển sang `true` và vận
hành topic có retention dài hơn.

### 9.8 Không monitor chuỗi CDC

Không có DAG hay task nào poll trạng thái Debezium connector, Kafka
consumer lag, hay Postgres replication slot lag. Lỗi trong chuỗi CDC
chỉ phát hiện gián tiếp qua `anomaly_check` hoặc khi inspect
`gold_dq_row_count_trend`.

---

## 10. Roadmap / dự kiến (chưa implement)

Phần này mô tả những gì design draft (`docs_update.md` + `new_doc.md`)
đề xuất nhưng **chưa có trong code hiện tại**. Mỗi mục nên được
implement trong một PR riêng với test riêng.

### 10.1 Hiện tại vs dự kiến

Pipeline hiện tại chạy như một DAG đơn, gần như không monitor. Quality
gate và continuous monitoring đã được document trong design draft nhưng
chưa implement.

```mermaid
flowchart LR
    subgraph TODAY["Hôm nay (code đang chạy)"]
        direction LR
        T1["spark_batch + spark_streaming"]
        T2["trino_bootstrap"]
        T3["dbt_build"]
        T4["gold_export"]
        T5["materialize_to_postgres"]
        T6["superset_bootstrap"]
        T7["anomaly_check<br/>(exit 0 luôn)"]
        T8["analytics_check<br/>(10 câu SQL)"]
        T1 --> T2 --> T3
        T3 --> T4
        T3 --> T5 --> T6
        T3 --> T7
        T6 --> T8
    end

    subgraph TOMORROW["Dự kiến (trong design, chưa code)"]
        direction LR
        P0["validate_raw_files<br/>[PLANNED #5]"]
        P1A["verify_silver"]
        P1B["verify_gold"]
        P1C["verify_postgres"]
        P1D["verify_superset"]
        P1E["verify_freshness"]
        P2A["monitor_dag @5min<br/>check_silver/gold/postgres/<br/>superset/freshness/<br/>pg_cdc/debezium/kafka"]
        P3A["pipeline_health.checks<br/>(bảng Postgres)"]
        P3B["Superset Pipeline<br/>Health Dashboard"]
        P4A["Slack + Email alerts"]
        P0 -.->|"block on fail"| T1
        T1 -.->|"gates"| P1A
        T4 -.->|"gates"| P1B
        T5 -.->|"gates"| P1C
        T6 -.->|"gates"| P1D
        T8 -.->|"gates"| P1E
        P2A -->|"ghi"| P3A -->|"đọc"| P3B
        P1A & P1B & P1C & P1D & P1E & P2A -.->|"FAIL"| P4A
    end

    style TODAY fill:#e8f5e9,stroke:#2e7d32
    style TOMORROW fill:#fff3e0,stroke:#c94,stroke-dasharray:5 3
```

**Chú thích**:

- **Nét liền** = code thực sự đang chạy hôm nay.
- **Nét đứt** = chưa implement; design draft đề xuất.
- **Hộp xanh** = main DAG hiện tại (không có quality gate, không có
+  continuous monitor).
- **Hộp cam** = các thành phần dự kiến (5 verify gate + 1 monitor DAG
+  + 1 health table + 1 health dashboard + 1 alert channel).

### 10.2 Năm quality gate (dự kiến)

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

Ngữ nghĩa gate:

| Gate | Đọc từ | Block ai | Kích hoạt bởi | Slack khi fail |
|---|---|---|---|---|
| `verify_silver` | Trino `hive.nyc.trips` | `trino_bootstrap` | mỗi DAG run | có + Email |
| `verify_gold` | Trino `hive.nyc_gold.*` | (chưa có) | mỗi DAG run | có + Email |
| `verify_postgres` | Postgres `nyc_analytics.public.*` | `superset_bootstrap` | mỗi DAG run | có + Email |
| `verify_superset` | Superset API | (không — Superset là bước cuối) | mỗi DAG run | có (warning) |
| `verify_freshness` | Trino `hive.mart.fact_trips` | `analytics_check` | mỗi DAG run | có + Email |

`verify_superset` là warning thay vì block vì dashboard là bước cuối
trong pipeline — không có downstream để block.

### 10.3 Monitor DAG (dự kiến)

```mermaid
flowchart TD
    TRIG["TRIG: @every 5 phút"]
    subgraph CHECKS["8 check chỉ-đọc (không ghi)"]
        C1["check_silver<br/>row count, null ratio, distribution"]
        C2["check_gold<br/>30 tables, row counts"]
        C3["check_postgres<br/>pg rows = gold rows"]
        C4["check_superset<br/>charts render OK"]
        C5["check_freshness<br/>MAX(date) le 35d"]
        C6["check_pg_cdc<br/>WAL size, replication slot"]
        C7["check_debezium<br/>connector RUNNING? lag < 5 phút?"]
        C8["check_kafka<br/>broker health, consumer lag"]
    end
    AGG["Tổng hợp thành PASS / WARN / FAIL"]
    DB[("Postgres<br/>pipeline_health.checks")]
    DASH["Superset Pipeline<br/>Health Dashboard"]
    ALERT["Slack + Email<br/>(chỉ CRITICAL)"]

    TRIG --> C1 & C2 & C3 & C4 & C5 & C6 & C7 & C8
    C1 & C2 & C3 & C4 & C5 & C6 & C7 & C8 --> AGG
    AGG --> DB
    DB --> DASH
    AGG -->|FAIL| ALERT
```

Đặc tính chính:

- **Chỉ đọc** — không bao giờ ghi vào data lake hay catalog. An toàn
+  chạy song song với main pipeline.
- **Độc lập** — Monitor DAG fail không block hay ảnh hưởng main pipeline.
+  Airflow coi hai DAG là các run độc lập.
- **Nhanh** — Mỗi check là một SELECT hoặc REST call. Tổng thời gian
+  dự kiến dưới 30 giây.
- **Chi tiết** — Mỗi check có severity riêng (PASS / WARN / FAIL) và
+  alert channel riêng.

### 10.4 Cross-node reconciliation (dự kiến)

```mermaid
flowchart TD
    subgraph RECON["4 check đối chiếu (block khi lệch)"]
        R1["R1: spark_input_rows<br/>== silver_rows + quarantine_rows"]
        R2["R2: silver_rows<br/>== mart.fact_trips_rows"]
        R3["R3: mart.* rows<br/>== gold_export.* rows"]
        R4["R4: gold.* rows<br/>== postgres.* rows"]
    end
    S1["output spark_batch"]
    SIL["nyc-silver"]
    QUA["nyc-quarantine"]
    MARTS["hive.mart.*"]
    GOLD["nyc-gold/*"]
    PG["Postgres analytics"]
    OK["ALL CLEAR<br/>Superset an toàn để hiển thị"]
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

Mỗi `R*` chạy như một task KubernetesPodOperator giữa các task pipeline
tương ứng. Row count so sánh qua `SELECT COUNT(*)` mỗi bên; tolerance = 0
(khớp chính xác).

### 10.5 Roadmap 16 mục (chi tiết)

16 mục dưới đây nhóm theo layer pipeline. Effort ước tính cho một
developer làm part-time.

| # | Tính năng | Layer | Nguồn | Effort |
+|---|---|---|---|---|
| | **Quality gates (nhóm A)** | | | |
| 1 | **Quality gates** — 5 inline task `verify_*` (`verify_silver`, `verify_gold`, `verify_postgres`, `verify_superset`, `verify_freshness`) block DAG khi fail | main DAG | `docs_update.md` §2 | 1 tuần |
| 3 | **Cross-node reconciliation** — 4 check row count (spark_input == silver + quarantine, silver == mart.fact_trips, mart.* == gold.*, gold.* == postgres.*) | main DAG | `docs_update.md` §4 | 3 ngày |
| 13 | **Output contracts YAML** — contract khai báo per-node (`contracts/silver.yaml`, `contracts/gold.yaml`, …) được verify task dùng | main DAG | `AGENTS.md` Output Contracts | 3 ngày |
| | **Monitoring (nhóm B)** | | | |
| 2 | **Monitor DAG** — DAG riêng chạy mỗi 5 phút với 8 check chỉ-đọc (silver / gold / postgres / superset / freshness / pg_cdc / debezium / kafka) → Slack + Email khi fail | monitor DAG | `docs_update.md` §2 | 1 tuần |
| 4 | **Pipeline Health Dashboard** — Superset dashboard trên bảng `pipeline_health.checks` được monitor DAG populate mỗi 5 phút | monitor DAG | `AGENTS.md` Health Dashboard section | 1 tuần |
| 12 | **Alert integration** — Slack + Email khi DAG fail, OOM, anomaly, freshness violation | monitor DAG | `AGENTS.md` Failure Mode Coverage + `docs/15-dataops-roadmap.md` | 1 tuần |
| | **Data integrity (nhóm C)** | | | |
| 5 | **MinIO pre-ingest validation** — `scripts/validate_raw_files.py` quarantine Parquet hỏng trước khi `spark_batch` chạy | ingest | `docs_update.md` §12 | 3 ngày |
| 14 | **Spark batch crash recovery** — ghi vào `_tmp/` rồi atomic move sang `silver/trips/` khi thành công; kiểm row count sau retry | ingest | `docs_update.md` Spark Scalability section | 3 ngày |
| 15 | **Spark batch auto-split theo tháng** — khi pod < 8 GB RAM, tách thành 12 sub-task tuần tự thay vì một `local[*]` lớn | ingest | `docs_update.md` Spark Scalability section | 3 ngày |
| | **CDC chain (nhóm D)** | | | |
| 6 | **CDC chain hardening** — Postgres WAL size + slot monitor, Debezium `snapshot.mode=schema_only` + `delete.handling.mode=rewrite`, Kafka idempotent producer + `enable.idempotence` + retention dài hơn + DLQ, cdc_bridge group_id cố định + `auto.offset.reset=latest` | CDC | `docs_update.md` §8–11 | 1 tuần |
| | **Compute (nhóm E)** | | | |
| 7 | **Trino resource groups** — `gold_export` cap 2 concurrent × 3 GB, `adhoc` 3 × 2 GB; batched CTAS (3 × 10) với pause 30s giữa batch | catalog | `docs_update.md` §13 | 3 ngày |
| 8 | **dbt CI + incremental models + business tests** — GitHub Actions chạy `dbt build --target staging` trên PR, `fact_trips` `materialized='incremental'`, cross-model revenue assertions | transform | `docs_update.md` §14 | 1 tuần |
| | **Presentation (nhóm F)** | | | |
| 9 | **Superset hardening** — idempotent bootstrap (đã gần đúng), cache-bust API sau pipeline, chart SQL version control trong `superset/charts/`, secret rotation qua env var, dashboard export sang git | serve | `docs_update.md` §15 | 3 ngày |
| 10 | **Anomaly check upgrade** — multi-metric (fare / distance / revenue), 30-day rolling ± 3σ baseline, optional `--block` flag, Slack webhook | serve | `docs_update.md` §16 | 2 ngày |
| | **Infrastructure (nhóm G)** | | | |
| 11 | **Staging environment** — namespace + MinIO bucket + Postgres riêng cho pre-merge testing | infra | `docs/15-dataops-roadmap.md` | 2 tuần |
| 16 | **Production deployment** — Trino 1 coord + 2 workers, Kafka 3 brokers, Postgres CDC trên RDS Multi-AZ, MinIO → S3, Spark trên EMR/Glue | infra | `new_doc.md` Production Hardening + `docs_update.md` §8 | 2+ tuần |

**Tổng effort** (cộng theo nhóm):

+- Nhóm A (gate + contract): ~3 tuần
+- Nhóm B (monitor + dashboard + alert): ~3 tuần
+- Nhóm C (hardening ingest): ~1.5 tuần
+- Nhóm D (chuỗi CDC): ~1 tuần
+- Nhóm E (Trino + dbt): ~2 tuần
+- Nhóm F (Superset + anomaly): ~1 tuần
+- Nhóm G (infra + production): ~4+ tuần
- **Tổng: ~15 tuần** cho một developer part-time

### 10.6 Gap đã biết mà roadmap chưa cover

Đây là issue đang mở theo dõi trong `docs/issues.md` nhưng chưa lên
lịch. Chúng nằm ngoài 16 mục trên và cần thảo luận riêng:

+- `is_delete_operator_pod=False` chỉ set trên 2/13 DAG task
+  (`trino_bootstrap` và `cdc_seed`); 11 task còn lại vẫn drop pod log
+  khi xong. (§5.4 liệt kê task nào.)
+ CDC bridge's random `group_id` gây trùng event khi restart
+  (xem §9.3). Mục #6 fix một phần nhưng không phải lựa chọn
+  `group_id` bản thân nó.
+ Streaming và batch chia sẻ đường dẫn silver đơn lẻ, không có source
+  marker (xem §9.2). Roadmap đề xuất tách `nyc-silver/batch/trips` và
+  `nyc-silver/stream/trips`, nhưng chỉ như một phần của mục #6
+  (CDC hardening) và chưa chính thức hoá.
+ `scripts/check_anomaly.py` luôn trả exit 0 (chỉ thông tin), nên
+  không thể block DAG dù design muốn vậy. Mục #10 đề xuất flag
+  `--block`, nhưng code hiện tại cần wire-up trước.
+ Tên chuỗi lỗi validation khác nhau giữa batch và streaming
+  (xem §9.1). Roadmap không đề cập.

---

## 11. Thuật ngữ

| Thuật ngữ | Nghĩa |
|---|---|
| **silver** | Tầng "trusted" — mỗi dòng đã pass 10 (hoặc 11) validation rule. Partition theo `pickup_year/pickup_month`. |
| **quarantine** | Tầng "rejected" — cùng schema với silver cộng `validation_errors ARRAY<VARCHAR>` và `quarantine_ts`. Không partition. |
| **gold** | Tầng "presentation". 33 dataset Parquet sinh ra bởi `export_gold_to_minio.py` từ Trino views. |
| **mart** | Tầng trung gian aggregate trong dbt (8 views). |
| **staging** | Tầng dbt 1:1 cast-and-rename trên các bảng external thô (3 views). |
| **validators** | 10 business rule trong `spark_local_batch.py` và bộ song song trong `spark_stream_taxi_events.py`. |
| **trip_id** | `xxhash64(concat_ws('|', pickup_ts, pickup_location_id, dropoff_location_id))` — cùng định nghĩa ở batch và streaming nên hai nguồn có thể union. |

---

## 12. License & nguồn dữ liệu

- Code pipeline: xem project root cho license.
- Nguồn dữ liệu: [NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- Dataset test đính kèm trong repo: 3 tháng đầu 2024
  (tháng 1 / 2 / 3), ~8.4M valid + 1.07M invalid dòng.
