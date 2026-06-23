# Pipeline Issues & Fix Plan

## 🔴 CRITICAL — Đang gây lỗi thực tế

### 1. `dq_row_count_trend` duplicate — 2 bản giống hệt
**Hiện trạng**: Sau khi thêm `dbt/models/gold/gold_dq_row_count_trend.sql` (dbt model), `export_gold_to_minio.py` vẫn CTAS ra `hive.nyc_gold.dq_row_count_trend`. Cùng 1 SQL, 2 bảng, 2 lần query Trino.
```
hive.mart.gold_dq_row_count_trend   ← dbt model (mới)
hive.nyc_gold.dq_row_count_trend    ← CTAS gold_export (cũ, cần xóa)
```
**Fix**: Xóa entry `dq_row_count_trend` khỏi `GOLD_DATASETS` trong `export_gold_to_minio.py`. Giữ dbt model.  
**File**: `scripts/export_gold_to_minio.py`

### 2. `materialize_to_postgres` không atomic — DROP rồi INSERT
**Hiện trạng**: `DROP TABLE IF EXISTS` → `CREATE TABLE` → `INSERT`. Nếu INSERT fail → bảng trắng. Superset thấy undefined.
```python
pg_cur.execute(f'DROP TABLE IF EXISTS "{name}"')
pg_cur.execute(f'CREATE TABLE "{name}" (...)')
# ... INSERT
```
**Fix**: Đổi sang swap table pattern: tạo `_new`, INSERT, DROP cũ, RENAME.
**File**: `scripts/materialize_to_postgres.py`

### 3. CDC seed đọc chung file parquet với batch → duplicate data
**Hiện trạng**: `cdc_seed` đọc 5000 dòng từ `yellow_tripdata_2024-01.parquet` — cùng file batch dùng. Kết quả: 5000 dòng vào silver 2 lần qua 2 đường khác nhau.
```
Batch:        raw parquet → Spark → silver (8.4M dòng)
CDC:          raw parquet → Postgres CDC → Debezium → Kafka → Spark streaming → silver (5000 dòng trùng)
```
**Fix**: CDC seed dùng nguồn riêng (production Postgres) hoặc thêm `source='cdc'` column để dedup.  
**File**: `scripts/cdc_seed.py` (entrypoint), `jobs/spark_stream_taxi_events.py`

---

## 🟡 HIGH — Gây đau về lâu dài

### 4. 30 gold tables nhưng chỉ 4 dbt gold model
**Hiện trạng**: `export_gold_to_minio.py` định nghĩa 30+ dataset trong `GOLD_DATASETS`. 26 cái chỉ là `SELECT ... GROUP BY ... FROM gold_fact_trips` — aggregate đơn giản, không có business logic mới.
**Fix**: Chuyển các bảng cần thiết nhất thành dbt model (`kpi_*`, `route_*`, `ops_*`). Xóa các CTAS thừa khỏi gold_export.
**File**: `dbt/models/gold/` (thêm model), `scripts/export_gold_to_minio.py` (xóa CTAS)

### 5. `gold_export` + `materialize_to_postgres` chạy SQL giống hệt 2 lần
**Hiện trạng**: Cả 2 script cùng loop `GOLD_DATASETS`, cùng execute SQL lên Trino. gold_export CTAS ra MinIO parquet, materialize SELECT rồi INSERT vào Postgres. 30 dataset × 2 = 60 query mỗi run.
**Fix**: Sau khi fix #4 (đưa phần lớn thành dbt model), materialize chỉ cần `SELECT * FROM hive.mart.gold_*` thay vì chạy lại SQL phức tạp.
**File**: `scripts/materialize_to_postgres.py`

### 6. Không có retry policy riêng cho Spark tasks
**Hiện trạng**: `nyc_e2e_pipeline` có `retries=3`, nhưng spark_batch task dùng image `apache/spark:3.5.1` — không phải custom image. Nếu MinIO chưa sẵn sàng, Spark fail → retry 3 lần → vẫn fail vì MinIO vẫn chưa lên.
**Fix**: Thêm `wait_for_minio()` vào spark_local_batch.py, hoặc thêm `init_container` check MinIO trước khi chạy Spark.
**File**: `jobs/spark_local_batch.py`

---

## 🟢 NICE-TO-HAVE

### 7. `materialize_to_postgres` dependency sai trong DAG
**Hiện trạng**: `analytics_refresh` DAG có `dbt_build >> materialize_postgres` song song với `dbt_build >> gold_export`. Đúng về mặt kỹ thuật (materialize query thẳng Trino), nhưng sai về mặt logic — materialize nên chạy SAU gold_export để đảm bảo data pipeline hoàn chỉnh.
**Fix**: `dbt_build >> gold_export >> materialize_postgres`
**File**: `airflow/dags/nyc_analytics_refresh.py`, `airflow/dags/nyc_e2e_pipeline.py`

### 8. Không có unique constraint trên fact_trips
**Hiện trạng**: `mode("append")` ghi thêm mỗi lần chạy, không có dedup key. Re-run pipeline = duplicate toàn bộ silver.
**Fix**: Thêm dedup key `(pickup_ts, vendor_id, pickup_location_id, dropoff_location_id)` hoặc đổi sang `mode("overwrite")` cho từng partition.
**File**: `jobs/spark_local_batch.py`

### 9. Hardcoded credentials everywhere
**Hiện trạng**: `minio/minio123`, `admin/admin`, `postgres/postgres`, `analytics/analytics` hardcode trong 10+ file.
**Fix**: Chuyển sang K8s Secrets + env vars.
**File**: `charts/nyc-taxi/templates/`, Helm values, entrypoint scripts

---

## Execution Order

| # | Priority | Task | Effort |
|---|---|---|---|
| 1 | 🔴 | Xóa `dq_row_count_trend` khỏi GOLD_DATASETS | 5 phút |
| 2 | 🔴 | Atomic materialize (swap table) | 30 phút |
| 3 | 🟡 | Đổi CDC data source | 1 giờ |
| 4 | 🟡 | Gộp gold tables vào dbt models | 2 giờ |
| 5 | 🟡 | Materialize chỉ SELECT *, không chạy SQL phức tạp | 30 phút |
| 6 | 🔴 | Spark wait_for_minio trước khi chạy | 15 phút |
| 7 | 🟢 | Sửa DAG dependency chain | 5 phút |
| 8 | 🟢 | Dedup key cho Spark | 1 giờ |
| 9 | 🟢 | K8s Secrets | 2 giờ |
