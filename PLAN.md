# PLAN.md — NYC Taxi Pipeline (MinIO S3, cập nhật 2026-06-08)

## 1) Mục tiêu

Pipeline **Kafka-first** cho NYC Taxi, lưu trữ data layer qua **MinIO S3** (S3-compatible). Có 2 đường ingestion: **batch** (fast, dùng cho backfill/verify) và **streaming** (Kafka).

```text
[Batch]              [Streaming]
Raw Parquet          Kafka Producer (generator)
    ↓                        ↓
spark_local_batch    spark_stream_taxi_events   ← Spark Docker
    ↓                        ↓
    └──→ MinIO S3 (silver / quarantine) ←──┘
                    ↓
              Trino (Hive catalog + S3 connector)
                    ↓
              dbt-trino (15 models + 9 tests)
                    ↓
              Analytics SQL (10 câu) + Superset (4 charts + dashboard)
                    ↓
              Airflow orchestration
```

---

## 2) Entry point: Makefile
```bash
make infra-up           # Core: ZK, Kafka, MinIO, Spark
make infra-up-all       # Tất cả services (gồm Trino, dbt, Superset, Airflow)
make minio-setup        # Upload raw data lên MinIO (cần chạy 1 lần đầu)
make spark-batch        # Batch 3 tháng (lần lượt MONTH=01,02,03)
make trino-bootstrap    # Register tables từ S3 paths
make dbt-build          # dbt models + tests (PASS 24/24)
make superset-bootstrap # DB, dataset, 4 charts, dashboard
make verify-all         # Full 7-step pipeline verify (gồm CDC)
make cdc-verify         # CDC E2E: seed → register → bridge
```

Xem thêm `AGENTS.md` cho danh sách đầy đủ targets và hướng dẫn chi tiết.

## 3) Trạng thái hiện tại (verified 2026-06-08)
### Hạ tầng Docker Compose (16+ services, 6 profiles)

**Storage backend**: MinIO S3 (`s3a://`) cho raw, silver, quarantine, lookup data. Local filesystem chỉ cho Hive metastore và streaming checkpoints.
### Docker Compose profiles

| Profile | Services |
|---|---|
| default | ZK, Kafka, Kafka-UI, MinIO, Spark Master/Worker |
| tools | topic-init, topic-run, generator, quality-report, nyc_postgres, debezium, cdc-seed, cdc-register, cdc-bridge |
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

- `jobs/spark_local_batch.py` — đọc raw parquet từ **MinIO S3** + zone lookup, enrichment, validation, split valid/invalid
- Chạy bằng: `make spark-batch` (docker `apache/spark:3.5.1 local[*]`, `--packages hadoop-aws:3.3.4,aws-java-sdk-bundle:1.12.262`)
- Input từ `s3a://nyc-raw/...`, output vào `s3a://nyc-silver/trips` + `s3a://nyc-quarantine/invalid_trips`
- **Không cần flag `--s3`** — S3 là default
- Kết quả 3 tháng:
  | Month | Valid | Invalid |
  |:--|:--|:--|
  | 01 | 2,724,037 | 240,587 |
  | 02 | 2,719,926 | 287,600 |
  | 03 | 3,036,445 | 546,183 |
  | **Total** | **8,480,408** | **1,074,370** |

### Streaming pipeline (Kafka-first)

- `generator/taxi_event_generator.py` — đọc parquet batch, push JSON events lên Kafka topic `taxi.trip.events`
- `jobs/spark_stream_taxi_events.py` — Spark Structured Streaming consumer, enrichment, validation — **always S3 mode**
- E2E test: `make verify-e2e` (5000 events)
- Script: `scripts/local_e2e_test.sh` / `scripts/local_e2e_full_9_5m.sh`

### Trino (analytics query engine)

- Image: `trinodb/trino:435`
- Hive connector file-based HMS + S3 connector (`hive.s3.*` config), catalog `hive`
- Schema `nyc` với tables `trips` (partitioned) + `invalid_trips` (non-partitioned) + `taxi_zone_lookup` (CSV)
- S3 paths mặc định (không cần `S3_MODE=1`)
- `make trino-bootstrap` → register tables + sync partitions + smoke test
- Kết quả: trips = 8,480,408, invalid_trips = 1,074,370, taxi_zone_lookup = 265

### dbt (data transformation)

- `dbt-trino` 1.11.x, models `view` (HMS file-based không hỗ trợ rename)
- **15 models** (3 staging + 3 marts + 4 gold + 5 additional marts), 9 data tests
- Naming: `stg_` → staging, `dim_`/`fact_` → marts, `gold_` → gold layer, `mart_` → summaries
- `make dbt-build` → **PASS=24 WARN=0 ERROR=0**

### Superset (visualization)

- Image: `apache/superset:4.0.0`
- DB connection qua `sqlalchemy-trino` dialect
- Python bootstrap (`scripts/superset_bootstrap.py`) — idempotent, tránh lỗi API bash
- Kết quả: 1 DB (NYC Trino) + 1 dataset (fact_trips) + 4 charts + 1 dashboard (NYC Taxi Overview)

### Airflow (orchestration)

- Postgres metadata, LocalExecutor, Docker-in-Docker
- 2 DAGs: `nyc_analytics_refresh` (dbt → Superset → analytics), `nyc_e2e_pipeline` (full)
- PythonOperator gọi `subprocess.run(["docker", ...])` với absolute host paths

### Debezium CDC (Postgres → Kafka → events)

- Postgres 16 (`nyc_postgres`) với WAL logical replication, `wal_level=logical`
- Debezium Kafka Connect 2.5 (`nyc_debezium`) — Postgres connector, `ExtractNewRecordState` transform
- `scripts/cdc_seed.py` — seed Postgres từ parquet (5000 rows mặc định)
- `scripts/cdc_register_connector.py` — đăng ký connector qua REST API
- `scripts/cdc_bridge.py` — CDC topic → `taxi.trip.events` (định dạng tương thích Spark)
- Unwrap transform loại bỏ `before/after/op` envelope, bridge convert microsecond timestamps → string
- Makefile targets: `cdc-up`, `cdc-seed`, `cdc-register`, `cdc-bridge`, `cdc-verify`
- Verified: 500 CDC events → `taxi.trip.events` topic (8.5s)
---

## 4) Cấu trúc project
nyc_new/
├── Makefile              ← Entry point chính (40+ targets)
├── AGENTS.md             ← Hướng dẫn cho AI assistant
├── PLAN.md / GUIDE.md / workflow.md
├── docker-compose.yml    ← 16+ services, 6 profiles
├── .gitignore
│
├── jobs/                 # Spark processors (MinIO S3 default)
│   ├── spark_local_batch.py           # Batch backfill — reads S3, writes S3
│   └── spark_stream_taxi_events.py    # Streaming (Kafka → S3)
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
│   └── start_streaming_job_docker.sh
│
├── airflow/dags/
│   ├── nyc_analytics_refresh.py
│   └── nyc_e2e_pipeline.py
│
├── dbt/                  # Transformation layer
│   ├── dbt_project.yml / profiles.yml
│   ├── models/staging/  (3 models)
│   ├── models/marts/    (8 models)
│   ├── models/gold/     (4 models)
│   └── tests/           (4 test files, 9 tests)
│
├── docker/               # Dockerfiles + configs
│   ├── tools.Dockerfile / dbt.Dockerfile / airflow.Dockerfile
│   ├── entrypoint-*.sh (7 files)
│   ├── trino/etc/ (config.properties, hive.properties với S3, jvm.config, ...)
│   └── superset/ (entrypoint, bootstrap_superset.sh, superset_config.py)
│
├── sql/
│   ├── analytics_questions.sql    # 10 câu SQL
│   └── smoke_tests.sql
│
├── k8s/                  # Kubernetes manifests (kind cluster)
│   ├── spark/ / trino/ / dbt/ / airflow/ / superset/ / kafka/ / debezium/ / postgres-cdc/
│   └── storage/ / jobs/ / configs/
│
└── data/                 # (gitignored)
    ├── raw/ / silver/ / quarantine/ / checkpoints/ / trino-metastore/
```

---
## 5) Data Quality rules

- `pickup_ts` not null/valid
- `dropoff_ts` not null/valid
- `dropoff_ts` > `pickup_ts`
- `trip_distance` > 0
- `fare_amount` >= 0
- `total_amount` >= `fare_amount`
- `passenger_count` in [1..6]
- pickup/dropoff_location_id tồn tại trong `taxi_zone_lookup`

Invalid records → quarantine (không drop im lặng).

---

## 6) Kết quả verify (2026-06-08 — clean run, 3 months via MinIO S3)

| Bước | Kết quả |
|---|---|
| `make spark-batch` MONTH=01 | Valid: 2,724,037 / Invalid: 240,587 |
| `make spark-batch` MONTH=02 | Valid: 2,719,926 / Invalid: 287,600 |
| `make spark-batch` MONTH=03 | Valid: 3,036,445 / Invalid: 546,183 |
| `make trino-bootstrap` | Trips: 8,480,408 / Invalid: 1,074,370 / Zone: 265 |
| `make dbt-build` | PASS=24 WARN=0 ERROR=0 |
| `make verify-mart` | dim_zone=261, fact_trips=8.48M, mart_hourly=11,748 |
| `make verify-analytics` | PASS 10/10 |
| `make superset-check` | 1 DB + 1 dataset + 4 charts + 1 dashboard |
| **`make verify-all`** | **ALL 7/7 PASS** |
| `make verify-cdc` | Postgres 5000 rows + Debezium RUNNING + 2 topics OK |

## 7) Còn lại / Next

- [x] **MinIO S3 migration** (June 2026) — Spark batch/streaming, Trino, dbt đều dùng S3 paths mặc định
- [x] **Full data run**: 9.5M rows (3 tháng) end-to-end via MinIO S3 — verified 2026-06-08
- [ ] **Cloud migration**: S3 → AWS S3, Glue HMS → file-based HMS, marts `materialized='table'`
  - Helm chart cho K8s deployment
  - AWS S3 thay MinIO (EMRFS / S3A connector)
  - AWS Glue / Unity Catalog thay file-based Hive Metastore
  - EMR Serverless / EMR on EKS thay Spark local[*]
  - Amazon MWAA thay Airflow LocalExecutor
  - Amazon QuickSight thay Superset (hoặc Superset auth)

---

## 8) Ghi chú quan trọng

- **Makefile là entry point**: Không gõ docker commands thủ công.
- **MinIO S3 default**: Spark batch/streaming dùng `s3a://` paths mặc định. Không cần `--s3` flag hay `S3_MODE=1`.
- **MinIO credentials**: Hardcoded `minio/minio123` trong Spark config (`fs.s3a.*`), Trino catalog (`hive.s3.*`), và mc alias. Đổi ở tất cả chỗ nếu rotate.
- **MinIO network**: Spark container cần `--network nyc_new_default` để reach MinIO (đã có trong Makefile `spark-batch`).
- **dbt mart `view`**: Hive file-based HMS không hỗ trợ rename → tất cả marts là `view`. Dùng `materialized='table'` sẽ fail.
- **Trino 435**: `node.environment` + `node.data-dir` trong `node.properties`, không trong `config.properties`.
- **Trino auth**: Bỏ `http-server.authentication.type` cho local dev; Trino vẫn yêu cầu `X-Trino-User` header.
- **Superset CSRF**: `WTF_CSRF_ENABLED=False` trong `superset_config.py` để REST POST không cần CSRF token.
- **Superset chart**: `datasource_id` là dataset id, không phải database id. Phải tạo dataset trước.
- **sqlalchemy-trino**: Phải `pip install` trong Superset container (đã add vào entrypoint).
- **Airflow DIND**: Dùng absolute host paths (`/home/dwcks/vsf_gsm/nyc_new`) trong Docker CLI.
- **HMS mount**: `data/trino-metastore` phải `:rw` để `sync_partition_metadata` hoạt động.
- **`spark-batch-s3` / `spark-streaming-s3`**: Alias targets — giữ lại cho backward compat.