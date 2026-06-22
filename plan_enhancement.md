# Pipeline Enhancement Plan — From Demo to Semi-Enterprise

## Overview
Nâng cấp 3 thành phần để pipeline không còn là ETL đơn giản: incremental load, slowly changing dimensions, anomaly detection.

---

## 1. Incremental Spark Load (`jobs/spark_local_batch.py`)
**Hiện tại**: Mỗi lần chạy đọc toàn bộ `s3a://nyc-raw/yellow_taxi/year=*/month=*/*.parquet` → 8.4M dòng mỗi lần  
**Mục tiêu**: Chỉ xử lý dữ liệu mới dựa trên `--incremental` flag  

### Thiết kế
- Thêm `--incremental` flag + `--state-path` (lưu last processed partition)
- Logic: đọc max(pickup_year, pickup_month) từ silver → chỉ scan partition mới hơn
- Fallback: nếu không có `--incremental` hoặc silver rỗng → full scan

### File thay đổi
- `jobs/spark_local_batch.py` — thêm ~30 dòng incremental logic
- `airflow/dags/nyc_e2e_pipeline.py` — thêm `--incremental` flag cho lần chạy sau

---

## 2. SCD Type 2 — Taxi Zone Dimension (`dbt/models/marts/dim_zone.sql`)
**Hiện tại**: `dim_zone` = 265 dòng tĩnh từ CSV lookup  
**Mục tiêu**: Khi zone thay đổi (ví dụ zone 264/265 đổi từ Unknown → có tên), giữ lịch sử  

### Thiết kế
- Tạo `snap_dim_zone` dùng dbt snapshot strategy `timestamp`
- Thêm cột: `dbt_valid_from`, `dbt_valid_to`, `dbt_updated_at`
- Model `dim_zone` chuyển thành view từ snapshot với `is_current = true`

### File thay đổi
- `dbt/snapshots/snap_dim_zone.sql` — snapshot mới
- `dbt/models/marts/dim_zone.sql` — refactor thành view từ snapshot

---

## 3. Anomaly Alert in Airflow (`airflow/dags/`)
**Hiện tại**: `dq_row_count_trend` có cột `anomaly_flag` nhưng không ai đọc  
**Mục tiêu**: Tự động alert khi `anomaly_flag = 'ANOMALY_LOW'` hoặc `'ANOMALY_HIGH'`  

### Thiết kế
- Thêm `check_anomaly` task sau `analytics_check` trong cả 2 DAG (`e2e` + `analytics_refresh`)
- Query `dq_row_count_trend` → nếu có anomaly → log WARNING + callback
- Dùng `PythonOperator` (không cần K8s pod) — nhẹ, nhanh

### File thay đổi
- `airflow/dags/nyc_e2e_pipeline.py` — thêm anomaly_check task
- `airflow/dags/nyc_analytics_refresh.py` — thêm anomaly_check task
- `scripts/check_anomaly.py` — script query anomaly + print report

---

## Implementation Order
1. [ ] SCD Type 2 (dbt) — nhanh nhất, chỉ thêm snapshot  
2. [ ] Anomaly Alert (Airflow) — 1 script + 2 DAG lines  
3. [ ] Incremental Spark — phức tạp nhất, cần test lại pipeline
