# NYC Taxi Pipeline — Workflow & Services

## Tổng quan kiến trúc

Pipeline xử lý dữ liệu taxi NYC từ raw Parquet / Kafka streaming → Silver (Spark) → Catalog (Trino/Hive) → Marts (dbt) → Dashboard (Superset).

```
                    ┌──────────┐
                    │  MinIO   │ (chưa dùng, để dành)
                    └──────────┘
                                                          
Raw Parquet ──► Spark Batch ──► Silver Parquet ──► Trino (Hive) ──► dbt ──► Superset
                    │ (local[*])   (partitioned by          ▲           │
                    │               pickup_year/month)       │           │
Kafka ──► Spark Streaming ──► Silver Parquet ───────────────┘           │
         (taxi.trip.events)                                               │
                                                                          │
                    CDC Path:                                            │
Postgres ──► Debezium ──► Kafka ──► cdc-bridge ──► taxi.trip.events      │
                                                    (reuse streaming)    │
                                                                          │
                              Airflow (orchestrates jobs)
```

---

## I. Core Infrastructure (Docker Compose)

### 1. Zookeeper (`confluentinc/cp-zookeeper:7.6.1`)
- **Port**: 2181
- **Vai trò**: Quản lý cluster metadata cho Kafka broker.

### 2. Kafka (`confluentinc/cp-kafka:7.6.1`)
- **Port**: 9092 (container) / 29092 (host)
- **Vai trò**: Message broker cho streaming events và CDC.
- **Topics**:
  - `taxi.trip.events` — events chính
  - `taxi.trip.events.invalid` — events lỗi
  - `taxi.trip.events.dlq` — dead-letter queue
  - `nyc_cdc.public.trips` — CDC từ Debezium
- **Kafka UI** (`provectuslabs/kafka-ui`): port 8080

### 3. MinIO (`minio/minio`)
- **Port**: 9000 (S3 API) / 9001 (Console)
- **Vai trò**: Lưu trữ object (chưa tích hợp pipeline, dành cho tương lai).

### 4. Spark Master & Worker (`apache/spark:3.5.1`)
- **Master**: port 7077 (cluster), 8081 (web UI)
- **Worker**: 2 cores, 4GB RAM
- **Vai trò**: Cluster mode cho streaming job. Batch job chạy `local[*]` không cần worker.

---

## II. Data Processing Layer

### 5. Spark Batch (`spark_local_batch.py`)
**File**: `jobs/spark_local_batch.py`

**Mục đích**: Xử lý backfill từ raw Parquet files, không cần Kafka.

**Luồng xử lý**:
```
Input Parquet ─► Cast columns ─► Add trip_id (xxhash64) ─► Add metadata ─►
  Join zones (CSV lookup) ─► Validate rules ─► Split valid/invalid ─►
    Valid   → Silver (partitioned by pickup_year/month)
    Invalid → Quarantine
```

**Validation rules**:
| Rule | Điều kiện lỗi |
|------|--------------|
| `pickup_ts` | NULL |
| `dropoff_ts` | NULL |
| Trip duration | `dropoff_ts <= pickup_ts` |
| Trip distance | `<= 0` |
| Fare amount | `< 0` |
| Total vs fare | `total_amount < fare_amount` |
| Passenger count | NULL hoặc `NOT BETWEEN 1 AND 6` |
| Pickup zone | Không tìm thấy trong lookup |
| Dropoff zone | Không tìm thấy trong lookup |

**Output**:
- `data/silver/trips/` — Parquet, partition: `pickup_year=N/pickup_month=N`
- `data/quarantine/invalid_trips/` — Parquet, không partition

**CLI**:
```bash
spark-submit --master local[*] jobs/spark_local_batch.py \
  --input <path> --lookup <path> --silver <path> --quarantine <path>
```

### 6. Spark Streaming (`spark_stream_taxi_events.py`)
**File**: `jobs/spark_stream_taxi_events.py`

**Mục đích**: Consumer Kafka streaming events (`taxi.trip.events`), xử lý realtime.

**Khác biệt với batch**:
- Đọc từ Kafka (JSON) thay vì Parquet
- Schema `EVENT_SCHEMA` (StructType) định nghĩa 19 trường
- Dùng `foreachBatch` với `trigger(availableNow=True)` cho batch-mode consumption
- Validation tương tự batch job
- Output: append vào silver/quarantine parquet

**Stream format** (event JSON):
```json
{
  "event_id": "uuid",
  "event_timestamp": "2024-01-01 00:00:00",
  "vendor_id": 1,
  "pickup_datetime": "...",
  "dropoff_datetime": "...",
  "passenger_count": 1,
  "trip_distance": 3.5,
  ...
}
```

---

## III. CDC Pipeline (Debezium)

### 7. Postgres CDC (`postgres:16-alpine`)
- **Port**: 5433 (host) / 5432 (container)
- **Cấu hình đặc biệt**: `wal_level=logical`, `max_replication_slots=4` (cần cho Debezium)
- **Table**: `trips` với `REPLICA IDENTITY FULL`
- **Init script**: `docker/entrypoint-init-postgres.sh`
  - Tạo `trips` table với `SERIAL PRIMARY KEY`
  - Bật `REPLICA IDENTITY FULL` (Debezium cần có full old value)

### 8. Debezium (`debezium/connect:2.5`)
- **Port**: 8084 (REST API)
- **Vai trò**: CDC connector, đọc WAL từ Postgres → emit events vào Kafka topic `nyc_cdc.public.trips`
- **Register connector**: `scripts/cdc_register_connector.py`
  ```python
  POST /connectors/ {
    "name": "nyc-postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "nyc_postgres",
      "database.dbname": "nyc_taxi",
      "table.include.list": "public.trips",
      "plugin.name": "pgoutput",
      "slot.name": "nyc_taxi_slot"
    }
  }
  ```

### 9. CDC Seed (`scripts/cdc_seed.py`)
**Mục đích**: Đổ dữ liệu Parquet (5000 rows mặc định) vào Postgres `trips` table.

**Flow**:
```
Parquet ─► pandas DataFrame ─► rename columns ─► dropna ─►
  sample (max-rows) ─► TRUNCATE trips ─► to_sql (append)
```

### 10. CDC Bridge (`scripts/cdc_bridge.py`)
**Mục đích**: Consumer từ Debezium topic (`nyc_cdc.public.trips`), transform sang format `taxi.trip.events`, produce lại.

**Transform logic**:
```python
Debezium event (unwrap) → {
  "event_id": "<uuid>",
  "event_timestamp": "<ts>",
  "vendor_id": ...,
  "pickup_datetime": ...,
  # ... các field giống EVENT_SCHEMA
}
```

**Why Bridge?** Debezium emits CDC events có format riêng (envelope: `before`/`after`/`source`/`op`/`ts_ms`). Bridge converts về đúng format mà Spark streaming job hiểu, cho phép reuse cùng một streaming job.

---

## IV. Catalog & Query Layer

### 11. Trino Coordinator (`trinodb/trino:435`)
- **Port**: 8083
- **Catalog**: `hive` — kết nối Hive Metastore từ parquet files
- **Schema**: `nyc` (silver), `mart` (dbt views)
- **Tables**:
  - `hive.nyc.trips` — external, partitioned by pickup_year/month
  - `hive.nyc.invalid_trips` — external
  - `hive.nyc.taxi_zone_lookup` — external CSV

### 12. Trino Bootstrap (`scripts/trino_register.py`)
**Mục đích**: Register tables trong Hive catalog.

**Flow**:
```
DROP TABLE IF EXISTS → CREATE TABLE (external_location='...', format='PARQUET')
  → CALL hive.system.sync_partition_metadata(mode='FULL')
  → SELECT COUNT(*) để verify
```

**Zone lookup**: Được register riêng từ CSV với tất cả columns VARCHAR.

---

## V. Transformation Layer (dbt)

### Model Hierarchy

```
hive.nyc.trips (raw parquet via Trino)
  │
  ├── stg_trips       (cast columns, fix types)
  ├── stg_zones       (zone lookup)
  ├── stg_invalid_trips
  │
  ├── fact_trips      (trip_id, tip_rate, trip_duration_sec, derived fields)
  ├── dim_zone        (zone dimension)
  ├── fact_invalid_trips
  │
  ├── mart_revenue_by_day         (daily: trip count, revenue, avg fare/tip/distance)
  ├── mart_revenue_by_zone        (zone pair: trip count, revenue, avg)
  ├── mart_trips_by_hour          (hour × dow: count, revenue, avg duration)
  ├── mart_payment_type_summary   (by payment type: count, revenue)
  ├── mart_hourly_summary         (date × hour × borough: count, revenue, avg)
  │
  └── gold_fact_trips             (full fact with trip_id + source_file)
      gold_dim_zone               (zone dimension)
      gold_mart_revenue_by_day    (daily revenue)
      gold_mart_revenue_by_zone   (zone pair revenue)
```

**Lưu ý**: Tất cả models đều `materialized='view'` (do Hive HMS không support `RENAME TABLE`).

### Tests (9 tests)
- `not_null`: total_amount, pickup_ts, dropoff_ts, payment_type, trip_distance
- `accepted_values`: payment_type (1-6)
- `payment_type_range.sql`: singular test kiểm tra payment_type hợp lệ

---

## VI. Visualization (Superset)

### 13. Superset (`apache/superset:4.0.0`)
- **Port**: 8088 (admin/admin)
- **Bootstrap script**: `scripts/superset_bootstrap.py`
  - Register Trino database connection
  - Create dataset `fact_trips` từ schema `mart`
  - Create 4 charts:
    1. `trips_per_hour` (bar chart)
    2. `top_pickup_zones` (table)
    3. `borough_revenue` (bar chart)
    4. `daily_trips` (line chart)
  - Create dashboard "NYC Taxi Overview"

---

## VII. Orchestration (Airflow)

### 14. Airflow (`apache/airflow:2.9.1`)
- **Port**: 8085 (admin/admin)
- **Entrypoint**: `docker/entrypoint-airflow.sh` — role-based:
  - `webserver` → start Airflow webserver
  - `scheduler` → start scheduler
  - `init` → migrate DB + create admin user

### DAG: `nyc_e2e_pipeline`
**Schedule**: manual trigger

**Tasks**:
```
Batch (for each month) ─► Trino Bootstrap ─► dbt Build ─► Superset Bootstrap ─► Analytics Check
```

**Flow**:
1. `spark_batch_m01` → chạy spark-submit cho tháng 01
2. `spark_batch_m02` → chạy spark-submit cho tháng 02
3. `spark_batch_m03` → chạy spark-submit cho tháng 03
4. `trino_bootstrap` → register tables + sync partitions
5. `dbt_build` → dbt deps + seed + run + test
6. `superset_bootstrap` → register DB/dataset/charts/dashboard
7. `analytics_check` → run 10 SQL questions, assert all return rows

### DAG: `nyc_analytics_refresh`
**Schedule**: manual (hoặc `@hourly` trong production)

**Tasks**:
```
dbt_build → superset_bootstrap → analytics_check
```

### K8s version
Trên Kubernetes (kind), Airflow DAG tương tự nhưng dùng `kubectl apply` thay vì `docker run`.

---

## VIII. Complete Flow (Step by Step)

### **Batch Pipeline (Backfill)**

```bash
make infra-up              # Start core: ZK, Kafka, MinIO, Spark
make kafka-topics          # Create topics
make spark-batch           # Run batch for months 01, 02, 03
make trino-bootstrap       # Register tables in Hive catalog
make dbt-build             # dbt models + tests (24/24 PASS)
make superset-bootstrap    # Register DB, dataset, charts, dashboard
make verify-all            # Full verification
```

### **Docker Compose Flow (Make targets)**

| Step | `make` target | What happens |
|------|---------------|-------------|
| 1 | `infra-up` | Start ZK, Kafka, Kafka-UI, MinIO, Spark Master/Worker |
| 2 | `infra-up-all` | + Trino, dbt container, Superset, Airflow |
| 3 | `kafka-topics` | Create 3 topics (events, invalid, dlq) |
| 4 | `spark-batch` | Run Spark batch cho 3 tháng (mỗi tháng ~200s) |
| 5 | `trino-bootstrap` | DROP + CREATE tables trong hive.nyc |
| 6 | `dbt-build` | dbt deps → seed → run (15 views) → test (9 tests) |
| 7 | `verify-mart` | Row counts: dim_zone=261, fact_trips=~8.5M, etc. |
| 8 | `superset-bootstrap` | Register DB → dataset → 4 charts → dashboard |
| 9 | `verify-analytics` | 10 SQL queries → PASS 10/10 |
| 10 | `verify-all` | Full pipeline + Superset check |

### **CDC Pipeline (Debezium)**

```bash
make cdc-up            # Start Postgres + Debezium
make cdc-seed          # Seed 5000 rows into Postgres
make cdc-register      # Register Debezium connector
make cdc-bridge        # Bridge CDC events → taxi.trip.events
make cdc-verify        # Full CDC E2E verification
```

**CDC Flow**:
```
Postgres trips table
  │  (INSERT/UPDATE/DELETE)
  ▼
WAL (Write-Ahead Log)
  │  (Debezium captures via pgoutput plugin)
  ▼
Kafka topic: nyc_cdc.public.trips
  │  (Debezium envelope: before/after/op/ts_ms)
  ▼
cdc-bridge.py
  │  (transform → standard event format)
  ▼
Kafka topic: taxi.trip.events
  │  (Spark streaming consumer)
  ▼
Silver Parquet (append)
```

### **Kubernetes (kind) Flow**

```bash
make k8s-cluster       # Create kind cluster (3 nodes)
make k8s-images        # Build & load custom images
make k8s-deploy        # Deploy all services + PVCs
make k8s-pipeline      # Run all jobs in order
make k8s-verify        # Check data in Trino
make k8s-down          # Delete cluster
```

---

## IX. Data Directory Structure

```
data/
├── raw/
│   └── yellow_taxi/year=2024/
│       ├── month=01/yellow_tripdata_2024-01.parquet   (~48MB)
│       ├── month=02/yellow_tripdata_2024-02.parquet   (~48MB)
│       └── month=03/yellow_tripdata_2024-03.parquet   (~57MB)
├── silver/
│   └── trips/
│       └── pickup_year=2024/
│           ├── pickup_month=1/   (~86MB, ~2.7M rows)
│           ├── pickup_month=2/   (~86MB, ~2.7M rows)
│           └── pickup_month=3/   (~96MB, ~3.0M rows)
├── quarantine/
│   └── invalid_trips/   (~8MB, ~1M invalid rows)
├── lookup/
│   └── taxi_zone_lookup.csv       (265 zones)
├── checkpoints/                    (streaming offset tracking)
└── trino-metastore/                (Hive HMS database)
```

---

## X. Key Services Summary Table

| Service | Image | Port(s) | Container Name | Profiles | Restart |
|---------|-------|---------|----------------|----------|---------|
| Zookeeper | `cp-zookeeper:7.6.1` | 2181 | `nyc_zookeeper` | default | unless-stopped |
| Kafka | `cp-kafka:7.6.1` | 9092, 29092 | `nyc_kafka` | default | unless-stopped |
| Kafka UI | `kafka-ui:latest` | 8080 | `nyc_kafka_ui` | default | unless-stopped |
| MinIO | `minio:latest` | 9000, 9001 | `nyc_minio` | default | unless-stopped |
| Spark Master | `spark:3.5.1` | 7077, 8081 | `nyc_spark_master` | default | unless-stopped |
| Spark Worker | `spark:3.5.1` | 8082 | `nyc_spark_worker` | default | unless-stopped |
| Postgres CDC | `postgres:16-alpine` | 5433 | `nyc_postgres` | tools | unless-stopped |
| Debezium | `debezium/connect:2.5` | 8084 | `nyc_debezium` | tools | unless-stopped |
| Trino | `trino:435` | 8083 | `nyc_trino` | tools, trino | unless-stopped |
| Superset | `superset:4.0.0` | 8088 | `nyc_superset` | tools, superset | unless-stopped |
| Airflow PG | `postgres:16-alpine` | - | `nyc_airflow_postgres` | airflow | unless-stopped |
| Airflow | `nyc-airflow:k8s` | 8085 | `nyc_airflow_webserver` | airflow | unless-stopped |

**One-shot jobs** (`restart: "no"`): topic-init, generator, quality-report, cdc-seed, cdc-register, cdc-bridge, trino-bootstrap, dbt.

---

## XI. CLI Reference

### Docker Compose
```bash
make infra-up            # Start core
make infra-up-all        # Start everything
make spark-batch         # Batch backfill
make spark-streaming     # Submit streaming job
make dbt-build           # dbt models + tests
make verify-all          # Full pipeline
make clean-all           # Delete generated data
```

### Kubernetes (kind)
```bash
make k8s-cluster
make k8s-images
make k8s-deploy
make k8s-pipeline
make k8s-verify
make k8s-down
```

### Trino Queries
```sql
-- Check data
SELECT count(*) FROM hive.nyc.trips;
SELECT pickup_year, pickup_month, count(*) FROM hive.nyc.trips GROUP BY 1,2;

-- Marts
SELECT * FROM hive.mart.fact_trips LIMIT 10;
SELECT * FROM hive.mart.mart_revenue_by_day ORDER BY 1 DESC;

-- CDC
SELECT * FROM hive.nyc.invalid_trips LIMIT 10;
```
