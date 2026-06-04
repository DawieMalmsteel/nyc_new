# PLAN.md — Kafka-first NYC Taxi Pipeline (Local-first, cập nhật theo implementation thực tế)

## 1) Mục tiêu

Xây dựng pipeline **Kafka-first** cho NYC Taxi, chạy **toàn bộ local** trước khi đưa lên cloud.

Luồng chính:

```text
NYC Taxi Parquet (3 tháng)
    -> Kafka Producer (event generator) --profile tools--
    -> Kafka topic (taxi.trip.events / taxi.trip.invalid)
    -> Spark Structured Streaming (Docker) --profile default--
    -> Data Quality Validation
    -> Silver (valid) + Quarantine (invalid) - parquet local
    -> Trino (hive catalog) --profile tools,trino--
    -> dbt-trino (stg/dim/fact/mart) --profile tools,dbt--
    -> 10 analytics SQL questions
    -> Superset dashboard (4 charts) --profile tools,superset--
```

---

## 2) Trạng thái hiện tại (đã làm)

### Ghi chú: mọi số liệu bên dưới là output **thực tế** chạy trên docker stack hiện tại (đã verify `2026-06-04`).

### Hạ tầng local bằng Docker Compose

Đã chạy được các service:

- Zookeeper
- Kafka
- Kafka UI
- MinIO
- Spark Master
- Spark Worker

File: `docker-compose.yml`

### Download dữ liệu challenge

Đã có đủ 3 tháng + lookup:

- `yellow_tripdata_2024-01.parquet`
- `yellow_tripdata_2024-02.parquet`
- `yellow_tripdata_2024-03.parquet`
- `taxi_zone_lookup.csv`

Script: `scripts/download_data.sh`

### Kafka topic management

Đã có topic scripts:

- `taxi.trip.events`
- `taxi.trip.invalid`
- `taxi.trip.dlq`
- topic run theo timestamp cho e2e test

Scripts:

- `scripts/create_kafka_topics.py`
- `scripts/create_kafka_topics.sh`

### Producer: Parquet -> Kafka events

Đã làm xong `generator/taxi_event_generator.py`:

- đọc parquet theo batch (không load full vào RAM)
- normalize schema/cột
- publish JSON vào Kafka
- inject invalid records để test quality
- hỗ trợ nhiều file input + `max-events=-1` (full dataset)

Scripts gọi nhanh:

- `scripts/run_generator.sh`
- `scripts/run_generator_full.sh`

### Spark Streaming job (Docker Spark)

Đã làm `jobs/spark_stream_taxi_events.py`:

- consume Kafka
- parse JSON schema rõ ràng
- enrich metadata (`ingestion_ts`, `pickup_date/hour/year/month`, offset, partition)
- join `taxi_zone_lookup.csv` cho pickup/dropoff
- validate rules
- split valid/invalid
- ghi parquet:
  - `data/silver/trips`
  - `data/quarantine/invalid_trips`

Script submit Spark Docker:

- `scripts/start_streaming_job_docker.sh`

### E2E local test script

Đã làm `scripts/local_e2e_test.sh`:

- start infra
- create topics
- download/check data
- publish events
- run stream processor (Spark docker mặc định)
- generate quality report + test report

Reports:

- `reports/data_quality_report.md`
- `reports/local_test_report.md`

### Đã test thành công thực tế
```bash
MAX_EVENTS=1000 bash scripts/local_e2e_test.sh
```

Kết quả: **PASS** (sau khi Dockerize hoàn toàn)

- Valid: **952**
- Invalid: **48**
- Total: 1000
- Chạy qua `docker compose run --rm` (không cần Python/Java trên host)
- Invalid: 52
- Total: 1000

---

## 3) Xác nhận quy mô dữ liệu thực tế

Đã kiểm tra row count 3 file parquet:

- 2024-01: **2,964,624**
- 2024-02: **3,007,526**
- 2024-03: **3,582,628**

Tổng: **9,554,778** rows (~9.55M)

---

## 4) Vì sao dùng Spark trong Docker

Host đang dùng Java 26 gây lỗi tương thích Spark local.

Giải pháp đã áp dụng:

- chạy Spark hoàn toàn trong Docker image `apache/spark:3.5.1` (Java 11)
- submit job qua `spark-master`
- tránh phụ thuộc Java host

Kết quả: Spark streaming chạy ổn và e2e pass.

### Phase 1: Dockerize toàn bộ

Sau khi e2e pass bằng `USE_DOCKER_SPARK=1`, đã migrate sang **chạy hoàn toàn bằng Docker** (không cần Python/Java/venv trên host):

- `docker/tools.Dockerfile` — Python 3.11-slim + kafka-python + pandas + pyarrow + trino client
- `docker/entrypoint-topic-init.sh`, `docker/entrypoint-generator.sh`, `docker/entrypoint-quality.sh`, `docker/entrypoint-trino-bootstrap.sh`
- `docker/wait-kafka.sh`
- `scripts/create_per_run_topic.py` — đợi partition leader assignment
- Refactor `scripts/local_e2e_test.sh` → chỉ gọi `docker compose run --rm`
- Thêm services vào `docker-compose.yml` dưới `profiles: ["tools"]`: `topic-init`, `topic-run`, `generator`, `quality-report`

---

## 5) Cấu trúc project hiện tại

```text
nyc_new/
├── PLAN.md
├── README.md
├── docker-compose.yml
├── config/
│   └── pipeline.yml
├── data/
│   ├── raw/yellow_taxi/year=2024/month=01..03/*.parquet
│   ├── lookup/taxi_zone_lookup.csv
│   ├── silver/trips/                # partitioned pickup_year/pickup_month
│   ├── quarantine/invalid_trips/
│   ├── trino-metastore/             # HMS file-based (rw, cho Trino)
│   └── checkpoints/
├── generator/
│   ├── taxi_event_generator.py
│   └── requirements.txt             # +trino client
├── jobs/
│   ├── spark_stream_taxi_events.py
│   ├── kafka_stream_processor.py          # fallback local processor
│   ├── spark_quality_report.py
│   └── spark_batch_backfill.py
├── docker/
│   ├── tools.Dockerfile             # python:3.11 + kafka-python + pandas + trino
│   ├── dbt.Dockerfile               # dbt-trino>=1.7,<2.0
│   ├── wait-kafka.sh
│   ├── entrypoint-topic-init.sh
│   ├── entrypoint-generator.sh
│   ├── entrypoint-quality.sh
│   ├── entrypoint-dbt.sh
│   ├── entrypoint-trino-bootstrap.sh
│   ├── trino/etc/
│   │   ├── config.properties        # coordinator, http port 8080
│   │   ├── node.properties          # environment + data-dir
│   │   ├── jvm.config
│   │   ├── log.properties
│   │   ├── password.db              # (unused, file-based no-auth in 435)
│   │   └── catalog/hive.properties  # HMS file-based, warehouse=file:///data/silver
│   └── superset/
│       ├── entrypoint-superset.sh   # db upgrade + admin + init + webserver bg
│       ├── bootstrap_superset.sh    # REST API: login + DB + dataset + 4 charts + dashboard
│       └── superset_config.py       # CSRF off, real SECRET_KEY
├── scripts/
│   ├── download_data.sh
│   ├── create_kafka_topics.py
│   ├── create_kafka_topics.sh
│   ├── create_per_run_topic.py      # mới: đợi partition leader
│   ├── run_generator.sh
│   ├── run_generator_full.sh
│   ├── start_streaming_job.sh             # host spark (legacy)
│   ├── start_streaming_job_docker.sh      # dùng thực tế
│   ├── local_e2e_test.sh            # refactored: chỉ gọi `docker compose run --rm`
│   ├── local_e2e_full_9_5m.sh
│   ├── trino_register.py            # mới: CREATE SCHEMA + CREATE TABLE trips/invalid_trips
│   ├── trino_sync_partitions.py     # mới: CALL hive.system.sync_partition_metadata (named args)
│   └── run_analytics_questions.py   # mới: chạy 10 câu SQL, PASS/FAIL
├── dbt/                              # mới
│   ├── dbt_project.yml               # marts:+materialized:view (HMS file rename limit)
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/stg_trips.sql
│   │   ├── staging/stg_invalid_trips.sql
│   │   ├── marts/dim_zone.sql
│   │   ├── marts/fact_trips.sql
│   │   ├── marts/fact_invalid_trips.sql
│   │   └── marts/mart_hourly_summary.sql
│   ├── tests/
│   │   ├── stg_trips_tests.yml
│   │   ├── fact_trips_tests.yml
│   │   ├── fact_invalid_trips_tests.yml
│   │   └── payment_type_range.sql
│   └── logs/
├── reports/
│   ├── data_quality_report.md
│   └── local_test_report.md
└── sql/
    ├── analytics_questions.sql       # 10 câu
    └── smoke_tests.sql
```

---

## 6) Cách chạy hiện tại

### A. Pipeline Kafka + Spark (e2e, default profile)

Quick test (1k records, full Docker):
```bash
docker compose up -d zookeeper kafka spark-master spark-worker
MAX_EVENTS=1000 bash scripts/local_e2e_test.sh
```

Full run (~9.55M records):
```bash
bash scripts/local_e2e_full_9_5m.sh
```

### B. Trino + dbt + Superset (analytics layer)

```bash
# 1. Khởi động infra + Trino + Superset
docker compose --profile tools --profile trino --profile dbt --profile superset up -d

# 2. Re-register Trino tables từ silver parquet
docker compose --profile tools --profile trino run --rm trino-bootstrap

# 3. Chạy dbt build (models + tests)
docker compose --profile tools --profile dbt run --rm dbt

# 4. (Tuỳ chọn) Re-run 10 analytics SQL questions
python3 scripts/run_analytics_questions.py

# 5. (Tuỳ chọn) Re-bootstrap Superset DB + 4 charts + dashboard
docker exec nyc_superset bash /app/docker/bootstrap_superset.sh
```

UI:
- Superset: http://localhost:8088 (admin/admin), dashboard `nyc-taxi`
- Trino:    http://localhost:8083 (no-auth trong 435 khi `http-server.authentication.type` không đặt)

---

## 7) Data Quality rules đang áp dụng

- `pickup_datetime` not null/valid
- `dropoff_datetime` not null/valid
- `dropoff > pickup`
- `trip_distance > 0`
- `fare_amount >= 0`
- `total_amount >= fare_amount`
- `passenger_count in [1..6]`
- pickup/dropoff location tồn tại trong `taxi_zone_lookup`

Invalid records được ghi vào quarantine, không drop im lặng.

---

## 8) Trạng thái next phase (Trino + dbt + Superset)

### Đã làm xong (verified 2026-06-04)

| Phase | Deliverable | Verified |
|---|---|---|
| Trino | coordinator:8083, hive catalog, tables `trips` (952 rows) + `invalid_trips` (0 rows) | `trino.dbapi.connect` → 952 |
| dbt | 6 models (stg_trips, stg_invalid_trips, dim_zone, fact_trips, fact_invalid_trips, mart_hourly_summary) + 9 tests | `dbt build` → PASS=15, ERROR=0 |
| Analytics | 10 SQL questions vs `hive.mart.*` | `scripts/run_analytics_questions.py` → PASS 10/10 |
| Superset | 1 DB (Trino), 1 dataset (fact_trips), 4 charts, 1 dashboard | `GET /api/v1/{database,chart,dashboard,dataset}/` → 1,4,1,1 |

### Còn lại (chưa làm)

- Airflow orchestration DAG (đặt lịch pipeline + dbt + Superset refresh)
- Debezium CDC pipeline (Postgres → Kafka → lake)
- Cloud migration: S3 thay local FS, Glue HMS thay file-based, marts `materialized='table'`
- Superset auth (bật lại PASSWORD/JWT) trước khi đưa lên cloud
- Re-run pipeline ở quy mô 9.5M rows end-to-end

---

## 9) Definition of Done (local hiện tại)

### Layer 1 — Kafka + Spark

- [x] Docker stack Kafka + Spark + MinIO chạy được (profile default)
- [x] Producer đẩy event từ NYC parquet vào Kafka (`docker compose run --rm generator`)
- [x] Spark docker consume Kafka, ghi silver/quarantine
- [x] Data quality report sinh ra được (`docker compose run --rm quality-report`)
- [x] E2E script trả về PASS (1000 events → 952 valid + 48 invalid)

### Layer 2 — Trino + dbt + Superset

- [x] Trino coordinator khởi động, catalog `hive` đọc được parquet từ `./data/silver` và `./data/quarantine`
- [x] `trips` table có 952 rows, `invalid_trips` table có 0 rows
- [x] `dbt build` PASS 15/15 (6 models + 9 tests)
- [x] 10/10 analytics SQL questions chạy được, mỗi câu trả về ≥1 row
- [x] Superset có 1 DB (Trino) + 1 dataset (fact_trips) + 4 charts + 1 dashboard

Trạng thái: **ĐẠT** cho đầy đủ local pipeline (Kafka + Spark + Trino + dbt + Superset).

---

## 10) Ghi chú quan trọng

- Với dữ liệu lớn (~9.55M rows), luôn ưu tiên chạy Spark trong Docker để tránh lỗi môi trường host.
- Nên giữ thói quen topic/checkpoint theo từng run khi test để tránh xung đột offset.
- Nếu cần benchmark throughput, tăng dần: partitions Kafka, số cores/memory Spark worker, batch size producer, flush interval producer.
- **Trino 435 split config**: `node.environment` + `node.data-dir` phải nằm trong `node.properties` (KHÔNG trong `config.properties`); thiếu sẽ crash vòng lặp.
- **HMS file-based read-only bug**: mount `./data` `:ro` → `CALL sync_partition_metadata` fail với `RenameOperation`. Phải mount `:rw`.
- **dbt mart `view` materialization**: hive file-HMS không hỗ trợ `CREATE OR REPLACE` rename, nên tất cả marts phải `view` (set trong `dbt_project.yml`). Cloud (HMS Glue/Unity) đổi lại `table`.
- **Trino sync partition args**: phải dùng named args `CALL hive.system.sync_partition_metadata(schema_name => 'nyc', table_name => 'trips', mode => 'FULL')`; positional bị ignore.
- **Trino 435 auth**: không có `ALLOW_ALL` type. Local dev: bỏ hẳn `http-server.authentication.type`, Trino vẫn chạy, client gửi `X-Trino-User` header (bắt buộc kể cả no-auth). SQLAlchemy URI: `trino://analytics@trino-coordinator:8080/hive/mart`.
- **Superset CSRF**: cần `WTF_CSRF_ENABLED=False` trong `superset_config.py` (load qua `PYTHONPATH=/app/docker`) để bootstrap REST POST không cần CSRF token.
- **Superset chart API**: `datasource_type='table'` yêu cầu `datasource_id` là **dataset id** (không phải database id). Phải tạo dataset trước bằng `POST /api/v1/dataset/` với `{database, schema, table_name}`, rồi mới tạo chart.
