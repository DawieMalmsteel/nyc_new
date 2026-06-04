# NYC Taxi Pipeline Flow & Technology Stack

## 1. High-Level Architecture
Pipeline được thiết kế theo mô hình **Kafka-first**, xử lý dữ liệu theo thời gian thực (streaming) và chuyển đổi thành Data Marts để phân tích thông qua lớp truy vấn SQL và trực quan hóa.

## 2. Technology Stack
- **Ingestion/Messaging:** Apache Kafka (Zookeeper, Kafka Topics).
- **Stream Processing:** Apache Spark Structured Streaming (Dockerized).
- **Storage Layer:** Local Filesystem (Parquet), MinIO (S3-compatible).
- **Query Engine:** Trino (v435, Hive connector với file-based HMS).
- **Transformation (ELT):** dbt-trino (View materialization).
- **Visualization:** Apache Superset.
- **Orchestration:** Apache Airflow (DAGs, LocalExecutor).
- **Orchestration & Env:** Docker & Docker Compose (với `profiles` để quản lý service).

## 3. Visual Pipeline Flow

```text
 [Raw Parquet] 
       |
 (Taxi Generator)
       |
       v
  +---------+     +------------------+     +------------------+
  |  Kafka  | --> | Spark Streaming  | --> | Parquet (Silver) |
  +---------+     +------------------+     +------------------+
                                                   |
                                                   v
  +-----------+     +------------------+     +------------+
  | Superset  | <-- | Trino + dbt      | <---| Parquet (Q)|
  +-----------+     +------------------+     +------------+
        ^                                          ^
        |                                          |
        +----------[ Airflow Orchestrator ]--------+
                  (Docker Compose / Exec)
```

## 4. Data Flow
1. **Producer:** `taxi_event_generator.py` đọc file Parquet thô (`/data/raw`) và đẩy JSON events vào topic `taxi.trip.events`.
2. **Stream Processing (Spark):** `spark_stream_taxi_events.py` đọc từ Kafka, validate dữ liệu (Schema, Rules), và phân loại:
   - Dữ liệu hợp lệ → `/data/silver/trips`
   - Dữ liệu lỗi → `/data/quarantine/invalid_trips`
3. **Query Engine (Trino):** Cung cấp giao diện SQL lên các file Parquet (`silver` & `quarantine`).
4. **Transformation (dbt):** dbt chạy các models (`staging`, `marts`) trên Trino để tạo ra các views:
   - `dim_zone`, `fact_trips`, `mart_hourly_summary`.
5. **Visualization (Superset):** Kết nối vào Trino để render các Dashboard phân tích (4 charts demo).
6. **Orchestration (Airflow):** Quản lý luồng chạy:
   - Trigger Spark job (nếu cần).
   - Trigger `trino-bootstrap` để đồng bộ metadata (sync partitions).
   - Trigger `dbt build` để cập nhật models.
   - Trigger `Superset bootstrap` để làm mới dashboard.

## 5. Orchestration Flow
Các DAG Airflow đóng vai trò "nhạc trưởng":
- `nyc_e2e_pipeline`: End-to-end cho luồng mới.
- `nyc_analytics_refresh`: Luồng định kỳ update mart + Superset.
- Task được thực thi thông qua `BashOperator` (calling `docker compose` / `docker exec`) để đảm bảo cô lập môi trường của từng công cụ.
