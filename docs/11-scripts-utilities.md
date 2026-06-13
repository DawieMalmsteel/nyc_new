# 11. Scripts và Tiện Ích

## 11.1 Tổng quan

Thư mục `scripts/` chứa 24 utility scripts phục vụ nhiều mục đích:
- Pipeline operations (Trino, CDC, Spark streaming)
- Verification (mart counts, analytics, CDC)
- Bootstrap (Superset datasets/charts/dashboard)
- Data management (gold export, partition sync)
- DevOps (port-forward, download, local E2E)

---

## 11.2 Trino Scripts

### trino_register.py

**Mục đích**: Đăng ký Hive external tables trong Trino catalog.

#### Kubernetes (Skaffold/Airflow) ⭐
Chạy qua `entrypoint-trino-bootstrap` trong KubernetesPodOperator:
```bash
# Airflow tự động chạy task trino_bootstrap
kubectl exec -n nyc-taxi -it deploy/trino -- trino --user analytics
```

#### Docker Compose (Legacy)
```bash
python3 scripts/trino_register.py
# ENV: TRINO_HOST, TRINO_PORT, S3_MODE, SILVER_PATH, ...
```

**Luồng xử lý:**
1. Chờ Trino ready (TCP connect, timeout 120s)
2. CREATE SCHEMA IF NOT EXISTS hive.nyc
3. Với mỗi table:
   - DROP TABLE IF EXISTS
   - CREATE TABLE WITH (external_location, format)
4. Sync partitions cho trips table
5. Smoke test (SELECT COUNT(*))

**Tables được tạo:**
- `hive.nyc.trips` (partitioned by pickup_year, pickup_month)
- `hive.nyc.invalid_trips` (non-partitioned)
- `hive.nyc.taxi_zone_lookup` (CSV external)

### trino_sync_partitions.py

**Mục đích**: Sync partition metadata trong Trino Hive catalog.

```bash
python3 scripts/trino_sync_partitions.py
```

Chạy:
```sql
CALL hive.system.sync_partition_metadata(
  schema_name => 'nyc',
  table_name => 'trips',
  mode => 'FULL'
)
```

Được gọi tự động trong `entrypoint-dbt.sh` trước khi chạy dbt build.

### export_gold_to_minio.py

**Mục đích**: Export datasets từ Trino sang MinIO S3 dưới dạng Parquet.

```bash
python3 scripts/export_gold_to_minio.py
# ENV: TRINO_HOST, TRINO_PORT, MINIO_ENDPOINT, ...
```

Xem chi tiết tại **docs/05-trino-catalog.md** (Section 5.4).

---

## 11.3 CDC Scripts

### cdc_seed.py

**Mục đích**: Đọc raw Parquet và insert vào Postgres trips table.

```bash
python3 scripts/cdc_seed.py \
  --input /opt/project/data/raw/.../yellow_tripdata_2024-01.parquet \
  --max-rows 5000 \
  --dsn postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi
```

**Chi tiết:**
- Đọc Parquet bằng Pandas
- Map columns: VendorID → vendor_id, tpep_pickup_datetime → pickup_datetime...
- Insert qua SQLAlchemy (chunk-based)
- Mặc định 5000 rows

### cdc_register_connector.py

**Mục đích**: Đăng ký Debezium Postgres connector qua REST API.

```bash
python3 scripts/cdc_register_connector.py \
  --debezium-url http://svc-debezium:8083 \
  --postgres-host svc-postgres-cdc
```

**Chi tiết:**
- POST connector config lên `${debezium_url}/connectors`
- Config gồm pgoutput plugin, ExtractNewRecordState transform
- Idempotent: DELETE + POST

### cdc_bridge.py

**Mục đích**: Bridge Debezium CDC events → standard taxi.trip.events format.

```bash
python3 scripts/cdc_bridge.py \
  --bootstrap-server svc-kafka:9092 \
  --input-topic nyc_cdc.public.trips \
  --output-topic taxi.trip.events
```

**Chi tiết:**
- Poll-based consumer loop với idle timeout
- Async Kafka send + periodic flush (500 events)
- Transform: unwrap Debezium envelope → flat event format
- Benchmark output at exit (events, time, throughput)

Xem chi tiết tại **docs/07-cdc-pipeline.md**.

---

## 11.4 Kafka Scripts

### create_kafka_topics.py

**Mục đích**: Tạo Kafka topics cho pipeline.

```bash
python3 scripts/create_kafka_topics.py \
  --bootstrap-server svc-kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

**Topics được tạo:**
| Topic | Partitions | Mục đích |
|-------|-----------|----------|
| `taxi.trip.events` | 3 | Main event stream |
| `taxi.trip.invalid` | 3 | Invalid events |
| `taxi.trip.dlq` | 3 | Dead letter queue |

### create_per_run_topic.py

**Mục đích**: Tạo topic tạm thời cho một run cụ thể.

```bash
python3 scripts/create_per_run_topic.py <bootstrap-server> <topic> <partitions>
```

Dùng cho các run testing với topic riêng.

### create_kafka_topics.sh

Shell wrapper cho `create_kafka_topics.py` (legacy).

---

## 11.5 Spark Streaming Scripts

### start_streaming_job.sh

**Mục đích**: Submit Spark streaming job từ host.

```bash
# Usage:
TOPIC=taxi.trip.events bash scripts/start_streaming_job.sh
```

K8s mode: `spark-submit` với master `spark://svc-spark-master:7077`.

### start_streaming_job_docker.sh

**Mục đích**: Submit Spark streaming job trong Docker Compose mode.

```bash
# Usage (từ Makefile):
TOPIC=taxi.trip.events bash scripts/start_streaming_job_docker.sh
```

Docker mode: `docker run` với `--network nyc_new_default`.

---

## 11.6 Verification Scripts

### run_analytics_questions.py

**Mục đích**: Chạy 10 analytics SQL queries, kiểm tra mỗi query trả về ≥1 row.

```bash
python3 scripts/run_analytics_questions.py
# ENV: TRINO_HOST, TRINO_PORT
```

**Output mẫu:**
```
[analytics] 10 questions found in analytics_questions.sql
[Q1] 2573 rows in 1.23s | first: ('Manhattan', 2573)
...
[analytics] PASS 10/10
```

Xem chi tiết tại **docs/08-superset-visualization.md** (Section 8.5).

### verify_mart.py

**Mục đích**: Đếm rows của 4 mart tables quan trọng.

```bash
python3 scripts/verify_mart.py
# ENV: TRINO_HOST, TRINO_PORT
```

**Tables verified:**
| Table | Expected rows |
|-------|--------------|
| `dim_zone` | ~261 |
| `fact_trips` | ~8-10M |
| `mart_hourly_summary` | ~11K+ |
| `mart_revenue_by_day` | ~90-96 |

### superset_check.py

**Mục đích**: Liệt kê tất cả resources trong Superset.

```bash
python3 scripts/superset_check.py
# ENV: SUPERSET_URL
```

### superset_dashboard_update.py / update_dashboard.py

**Mục đích**: Update dashboard configuration programmatically.

```bash
python3 scripts/superset_dashboard_update.py
```

---

## 11.7 Bootstrapping Scripts

### superset_bootstrap.py

**Mục đích**: Idempotent Superset setup — DB, datasets, charts, dashboard.

```bash
python3 scripts/superset_bootstrap.py
# ENV: SUPERSET_URL, TRINO_URI
```

Xem chi tiết tại **docs/08-superset-visualization.md**.

### run_dbt.sh

**Mục đích**: Shell wrapper cho dbt commands.

```bash
bash scripts/run_dbt.sh [build|run|test]
```

---

## 11.8 Data & E2E Scripts

### download_data.sh

**Mục đích**: Download dữ liệu NYC TLC (Parquet) từ nguồn.

```bash
bash scripts/download_data.sh
```

### local_e2e_full_9_5m.sh

**Mục đích**: Chạy full E2E pipeline local (9.5M rows).

```bash
bash scripts/local_e2e_full_9_5m.sh
```

### local_e2e_test.sh

**Mục đích**: Chạy E2E test nhanh local.

```bash
bash scripts/local_e2e_test.sh
```

---

## 11.9 DevOps Script

### k8s_ui.sh

**Mục đích**: Quản lý K8s port-forwards với auto-restart.

```bash
./scripts/k8s_ui.sh start   # Start port-forwards (setsid -f)
./scripts/k8s_ui.sh stop    # Stop all port-forwards
```

**Cơ chế:**
```bash
for mapping in "svc/svc-superset:39080:8088" "svc/svc-minio:39081:9000" ...; do
    setsid -f sh -c "
      while true; do
        kubectl port-forward --address 0.0.0.0 -n nyc-taxi $svc $lport:$rport > /dev/null 2>&1
        sleep 3  # Auto-restart if connection lost
      done
    "
done
```

**Đặc điểm:**
- Dùng `setsid -f` — process sống sau khi `make` thoát
- `--address 0.0.0.0` — cho phép truy cập từ máy khác
- Auto-restart loop khi connection lost
- Port range 39080-39087 (tránh conflict với kind NodePort 38080)

---

## 11.10 Run Generator Scripts

### run_generator.sh

**Mục đích**: Chạy Kafka producer generator (sinh events từ Parquet).

```bash
bash scripts/run_generator.sh
```

### run_generator_full.sh

**Mục đích**: Chạy generator với tất cả dữ liệu.

```bash
bash scripts/run_generator_full.sh
```

---

## 11.11 Script Dependencies Overview

```
scripts/
├── trino_register.py              # trino (pip)
├── trino_sync_partitions.py        # trino
├── export_gold_to_minio.py         # trino, minio (pip)
├── run_analytics_questions.py      # trino
├── verify_mart.py                  # trino
├── superset_bootstrap.py           # urllib (stdlib)
├── superset_check.py               # urllib
├── superset_dashboard_update.py    # urllib
├── cdc_bridge.py                   # kafka-python
├── cdc_seed.py                     # pandas, pyarrow, sqlalchemy
├── cdc_register_connector.py       # urllib
├── create_kafka_topics.py          # kafka-python
├── create_per_run_topic.py         # kafka-python
├── k8s_ui.sh                       # bash + kubectl
├── download_data.sh                # bash (wget/curl)
├── local_e2e_*.sh                  # bash (make targets)
├── run_generator*.sh               # bash
└── run_dbt.sh                      # bash (dbt CLI)
```
