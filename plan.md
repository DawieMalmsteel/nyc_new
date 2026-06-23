# Pipeline Issues & Fix Plan

## 🔴 CRITICAL — Đang gây lỗi thực tế

### 1. `dq_row_count_trend` duplicate — 2 bảng giống hệt
**Hiện trạng**: Sau khi thêm `dbt/models/gold/gold_dq_row_count_trend.sql`, `export_gold_to_minio.py` vẫn CTAS ra `hive.nyc_gold.dq_row_count_trend`. Cùng SQL, 2 bảng, 2 lần query Trino.
```
hive.mart.gold_dq_row_count_trend   ← dbt model (mới, đúng)
hive.nyc_gold.dq_row_count_trend    ← CTAS gold_export (cũ, trùng)
```
**Fix**: Không xóa khỏi `GOLD_DATASETS` (materialize cần nó để copy sang Postgres). Thay vào đó: đổi SQL trong `GOLD_DATASETS` thành `SELECT * FROM hive.mart.gold_dq_row_count_trend`. gold_export sẽ thấy SQL này và không CTAS nữa (đã là dbt model), materialize vẫn copy sang PG bình thường.
**File**: `scripts/export_gold_to_minio.py`

### 2. `materialize_to_postgres` không atomic — DROP rồi INSERT
**Hiện trạng**: `DROP TABLE IF EXISTS` → `CREATE TABLE` → `INSERT`. Nếu INSERT fail → bảng trắng, Superset thấy undefined.
**Fix**: Đổi sang swap pattern: `CREATE TABLE _new` → `INSERT INTO _new` → `DROP TABLE` → `ALTER TABLE _new RENAME TO`. Nếu fail ở bước INSERT, bảng cũ vẫn nguyên vẹn.
**File**: `scripts/materialize_to_postgres.py`

---

## 🟡 HIGH — Over-engineering / lãng phí

### 3. 30 gold tables CTAS nhưng chỉ 4 dbt gold model — query Trino 2 lần
**Hiện trạng**: `GOLD_DATASETS` định nghĩa 30+ bảng, 26 cái chỉ là `SELECT ... GROUP BY ... FROM gold_fact_trips`. Cả `gold_export` lẫn `materialize_to_postgres` đều chạy cùng SQL này lên Trino — 30 dataset × 2 = 60 query mỗi pipeline run. Lãng phí.
**Fix**: 
- Chuyển các bảng dùng nhiều nhất (`kpi_*`, `route_*`, `ops_*`) thành dbt model → chạy 1 lần trong `dbt build`
- Các bảng còn lại trong `GOLD_DATASETS` đổi SQL thành `SELECT * FROM hive.mart.gold_<name>` → không chạy lại SQL phức tạp
- Sau khi xong, materialize chỉ copy kết quả dbt model, không query lại Trino
**File**: `dbt/models/gold/` (thêm), `scripts/export_gold_to_minio.py` (sửa SQL)

### 4. Không có wait_for_minio trong Spark task
**Hiện trạng**: Spark batch task chạy ngay khi DAG trigger, không check MinIO sẵn sàng. Nếu MinIO chưa lên → Spark fail → retry 3 lần → vẫn fail.
**Fix**: Thêm `wait_for_minio()` vào `spark_local_batch.py`, poll `SELECT 1` từ MinIO health endpoint trước khi đọc dữ liệu.
**File**: `jobs/spark_local_batch.py`

---

## 🟢 GOLD LAYER — Migrate 40 bảng cũ → 20 bảng BI mới

### Hiện trạng
`GOLD_DATASETS` trong `export_gold_to_minio.py` có 30+ bảng CTAS, hầu hết là `GROUP BY ... FROM gold_fact_trips`. Trùng lặp, không business logic. Superset dùng các bảng này.

### Chiến lược migrate (không xóa ngang — giữ Superset chạy)

**Bước 1: Tạo 20 dbt model mới** — mỗi model trả lời 1 câu hỏi business (xem `plan_gold_layer.md`)

**Bước 2: Map bảng cũ → bảng mới**

| Bảng cũ (xóa) | Thay bằng dbt model mới |
|---|---|
| `fact_trips_daily` | `gold_executive_daily` |
| `fact_trips_hourly` | `gold_hourly_pulse` |
| `kpi_daily_overview` | `gold_executive_daily` |
| `kpi_weekly_trends` | `gold_executive_weekly` |
| `kpi_monthly_summary` | `gold_growth_metrics` |
| `kpi_vendor_performance` | `gold_vendor_battlecard` |
| `kpi_zone_performance` | `gold_zone_whitepaper` |
| `kpi_payment_trends` | `gold_payment_behavior` |
| `kpi_borough_comparison` | `gold_zone_demographics` |
| `kpi_zone_net_flow` | `gold_customer_journey` |
| `ops_passenger_count_pattern` | `gold_customer_segments` |
| `route_popular_routes` | `gold_customer_journey` |
| `od_borough_matrix` | `gold_customer_journey` |

**Bước 3: Giữ lại bảng không trùng, đổi SQL → SELECT \***

| Bảng cũ giữ lại | Lý do |
|---|---|
| `fact_trips_borough` | Map visualization |
| `fact_trips_hourly_zone` | Hourly × zone chi tiết |
| `ops_peak_hours_heatmap` | Bổ trợ hourly_pulse |
| `ops_trip_distance_distribution` | Phân phối khoảng cách |
| `ops_utilization_rate` | Bổ trợ trip_unit_economics |
| `route_airport_analysis` | Airport riêng |
| `route_airport_zone_matrix` | Airport chi tiết |
| `route_cross_borough` | Cross-borough flow |
| `route_top_pickup_zones` | Top pickup |
| `route_top_dropoff_zones` | Top dropoff |

**Bước 4: Dim tables giữ nguyên** — `dim_date`, `dim_payment_type`, `dim_rate_code`, `dim_vendor`, `dim_zone`, `dim_zone_grouped`

**Bước 5: Cập nhật Superset** — đổi dataset source từ bảng cũ sang bảng mới

**Kết quả**: 40 bảng → 20 mới (dbt) + 10 cũ SELECT * + 6 dim = 36 bảng. Không trùng, có business logic.

---

## Execution Order

| # | Task | Effort | Impact |
|---|---|---|---|
| 1 | Sửa `dq_row_count_trend` SQL trong GOLD_DATASETS | 5 phút | Hết duplicate |
| 2 | Atomic materialize (swap table) | 30 phút | Superset không thấy bảng trắng |
| 3 | Spark wait_for_minio | 15 phút | Spark không fail vô ích |
| 4 | Tạo 20 dbt gold model (P1: 13 bảng) | 2 giờ | Business dashboard |
| 5 | Map GOLD_DATASETS cũ → SELECT * từ model mới | 30 phút | Không query trùng |
| 6 | Cập nhật Superset dataset mapping | 30 phút | Dashboard dùng bảng mới |
| 7 | Tạo 7 dbt gold model (P2) | 1 giờ | Hoàn thiện BI |
