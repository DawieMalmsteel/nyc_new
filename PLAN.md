# PLAN.md — NYC Taxi Pipeline (Kafka-first, local, cập nhật 2026-06-05)

## 1) Mục tiêu

Pipeline **Kafka-first** cho NYC Taxi, chạy **toàn bộ local trước** khi đưa lên cloud.
Có 2 đường ingestion: **batch** (fast, dùng cho backfill/verify) và **streaming** (Kafka).

```text
[Batch]              [Streaming]
Raw Parquet          Kafka Producer (generator)
    ↓                        ↓
spark_local_batch    spark_stream_taxi_events   ← Spark Docker
    ↓                        ↓
    └──→ Parquet (silver / quarantine) ←──┘
                    ↓
              Trino (Hive catalog)
                    ↓
              dbt-trino (6 models + 9 tests)
                    ↓
              Analytics SQL (10 câu) + Superset (4 charts + dashboard)
                    ↓
              Airflow orchestration
```

---

## 2) Entry point: Makefile

```bash
make infra-up           # Core: ZK, Kafka, MinIO, Spark
make infra-up-all       # Tất cả services
make spark-batch        # Batch 2.7M rows (~30s)
make trino-bootstrap    # Register tables
make dbt-build          # dbt models + tests (PASS 15/15)
make superset-bootstrap # DB, dataset, 4 charts, dashboard
make verify-all         # Full 6-step pipeline verify
```

Xem thêm `AGENTS.md` cho danh sách đầy đủ targets và hướng dẫn chi tiết.

---

## 3) Trạng thái hiện tại (verified 2026-06-05)

### Hạ tầng Docker Compose (15 services, 6 profiles)

| Profile | Services |
|---|---|
| default | ZK, Kafka, Kafka-UI, MinIO, Spark Master/Worker |
| tools | topic-init, topic-run, generator, quality-report |
| trino | trino-coordinator, trino-bootstrap |
| dbt | dbt |
| superset | superset |
| airflow | airflow-postgres, airflow-init, airflow-webserver, airflow-scheduler |

File: `docker-compose.yml` — profile-based, volume `./:/opt/project`.

### Data source

- `yellow_tripdata_2024-01.parquet` (2,964,624 rows)
- `yellow_tripdata_2024-02.parquet` (3,007,526 rows)
- `yellow_tripdata_2024-03.parquet` (3,582,628 rows)
- `taxi_zone_lookup.csv` (262 zones)
- Tổng: **9,554,778 rows** (~9.5M)

### Batch pipeline (path chính cho verify)

- `jobs/spark_local_batch.py` — đọc raw parquet + zone lookup, enrichment, validation, split valid/invalid
- Kết quả: **2,724,037 valid** / **240,587 invalid** cho tháng 01/2024
- Chạy bằng: `make spark-batch` (docker `apache/spark:3.5.1 local[*]`)
- Permission: Spark UID 185 != host UID 1000 → dirs phải 777. `make setup-volumes` fix.

### Streaming pipeline (Kafka-first)

- `generator/taxi_event_generator.py` — đọc parquet batch, push JSON events lên Kafka topic `taxi.trip.events`
- `jobs/spark_stream_taxi_events.py` — Spark Structured Streaming consumer, enrichment, validation
- E2E test: `make verify-e2e` (5000 events)
- Script: `scripts/local_e2e_test.sh` / `scripts/local_e2e_full_9_5m.sh`

### Trino (analytics query engine)

- Image: `trinodb/trino:435`
- Hive connector file-based HMS, catalog `hive`
- Schema `nyc` với tables `trips` + `invalid_trips` từ parquet
- `make trino-bootstrap` → register tables + sync partitions + smoke test
- Kết quả: trips = 2,724,037, invalid_trips = 0 (batch)

### dbt (data transformation)

- `dbt-trino` 1.10.2, models `view` (HMS file-based không hỗ trợ rename)
- 6 models: `stg_trips`, `stg_invalid_trips`, `dim_zone`, `fact_trips`, `fact_invalid_trips`, `mart_hourly_summary`
- 9 data tests: not_null checks, payment_type range
- `make dbt-build` → **PASS=15 WARN=0 ERROR=0**

### Superset (visualization)

- Image: `apache/superset:4.0.0`
- DB connection qua `sqlalchemy-trino` dialect
- Python bootstrap (`scripts/superset_bootstrap.py`) — idempotent, tránh lỗi API bash
- Kết quả: 1 DB (NYC Trino) + 1 dataset (fact_trips) + 4 charts + 1 dashboard (NYC Taxi Overview)

### Airflow (orchestration)

- Postgres metadata, LocalExecutor, Docker-in-Docker
- 2 DAGs: `nyc_analytics_refresh` (dbt → Superset → analytics), `nyc_e2e_pipeline` (full)
- PythonOperator gọi `subprocess.run(["docker", ...])` với absolute host paths

---

## 4) Cấu trúc project

```text
nyc_new/
├── Makefile              ← Entry point chính (30+ targets)
├── AGENTS.md             ← Hướng dẫn cho AI assistant
├── PLAN.md / README.md / flow.md
├── docker-compose.yml    ← 15 services, 6 profiles
├── config/pipeline.yml
├── .gitignore
│
├── jobs/                 # Spark processors
│   ├── spark_local_batch.py           # Batch backfill (working)
│   ├── spark_stream_taxi_events.py    # Streaming (Kafka → silver)
│   ├── spark_batch_backfill.py        # (legacy)
│   ├── kafka_stream_processor.py      # Fallback local (legacy)
│   └── spark_quality_report.py
│
├── generator/            # Kafka event generator
│   ├── taxi_event_generator.py
│   └── requirements.txt
│
├── scripts/              # Pipeline utilities
│   ├── verify_mart.py                 # Row counts
│   ├── superset_bootstrap.py          # Idempotent Superset setup
│   ├── superset_check.py              # List Superset resources
│   ├── run_analytics_questions.py     # 10 analytics SQL
│   ├── trino_register.py / trino_sync_partitions.py
│   ├── create_kafka_topics.py / create_per_run_topic.py
│   ├── local_e2e_test.sh / local_e2e_full_9_5m.sh
│   ├── download_data.sh
│   ├── run_generator.sh / run_generator_full.sh
│   └── start_streaming_job.sh / start_streaming_job_docker.sh
│
├── airflow/dags/
│   ├── nyc_analytics_refresh.py
│   └── nyc_e2e_pipeline.py
│
├── dbt/                  # Transformation layer
│   ├── dbt_project.yml / profiles.yml
│   ├── models/staging/stg_trips.sql, stg_invalid_trips.sql
│   ├── models/marts/dim_zone.sql, fact_trips.sql, fact_invalid_trips.sql, mart_hourly_summary.sql
│   └── tests/ (4 test files, 9 tests)
│
├── docker/               # Dockerfiles + configs
│   ├── tools.Dockerfile / dbt.Dockerfile / airflow.Dockerfile
│   ├── entrypoint-*.sh (7 files)
│   ├── trino/etc/ (config.properties, hive.properties, jvm.config, ...)
│   └── superset/ (entrypoint, bootstrap_superset.sh, superset_config.py)
│
├── sql/
│   ├── analytics_questions.sql    # 10 câu SQL
│   └── smoke_tests.sql
│
└── data/                 # (gitignored)
    ├── raw/ / silver/ / quarantine/ / checkpoints/ / trino-metastore/
    └── lookup/taxi_zone_lookup.csv
```

---

## 5) Data Quality rules

- `pickup_datetime` not null/valid
- `dropoff_datetime` not null/valid
- `dropoff > pickup`
- `trip_distance > 0`
- `fare_amount >= 0`
- `total_amount >= fare_amount`
- `passenger_count in [1..6]`
- pickup/dropoff location tồn tại trong `taxi_zone_lookup`

Invalid records → quarantine (không drop im lặng).

---

## 6) Kết quả verify (2026-06-05)

| Bước | Kết quả |
|---|---|
| `make spark-batch` | Valid: 2,724,037 / Invalid: 240,587 |
| `make trino-bootstrap` | Trips: 2,724,037 |
| `make dbt-build` | PASS=15 WARN=0 ERROR=0 |
| `make verify-mart` | dim_zone=261, fact_trips=2.7M, mart_hourly=3,945 |
| `make verify-analytics` | PASS 10/10 |
| `make superset-check` | 1 DB + 1 dataset + 4 charts + 1 dashboard |
| **`make verify-all`** | **ALL 6/6 PASS** |

---

## 7) Còn lại / Next

- [ ] **Debezium CDC pipeline**: Postgres → Kafka → lake (new source integration)
- [ ] **Cloud migration**: S3 → local FS, Glue HMS → file-based HMS, marts `materialized='table'`
- [ ] **Superset auth**: Bật PASSWORD/JWT before cloud
- [ ] **Full data run**: 9.5M rows end-to-end (requires resources, deferred)

---

## 8) Ghi chú quan trọng

- **Makefile là entry point**: Không gõ docker commands thủ công.
- **Spark permission**: UID 185 (container) ≠ 1000 (host). `make setup-volumes` set 777.
- **dbt mart `view`**: Hive file-HMS không hỗ trợ rename → tất cả marts là `view`.
- **Trino 435**: `node.environment` + `node.data-dir` trong `node.properties`, không trong `config.properties`.
- **Trino auth**: Bỏ `http-server.authentication.type` cho local dev; Trino vẫn yêu cầu `X-Trino-User` header.
- **Superset CSRF**: `WTF_CSRF_ENABLED=False` trong `superset_config.py` để REST POST không cần CSRF token.
- **Superset chart**: `datasource_id` là dataset id, không phải database id. Phải tạo dataset trước.
- **sqlalchemy-trino**: Phải `pip install` trong Superset container (đã add vào entrypoint).
- **Airflow DIND**: Dùng absolute host paths (`/home/dwcks/vsf_gsm/nyc_new`) trong Docker CLI.
- **HMS mount**: `data/trino-metastore` phải `:rw` để `sync_partition_metadata` hoạt động.
