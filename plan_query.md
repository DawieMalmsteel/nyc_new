# Saved Queries Plan — NYC Taxi Gold Analytics

## Mục tiêu

Hiện tại Superset dashboard đã có 26 charts + 30 datasets trên Postgres analytics, nhưng **chưa có saved query** nào trong SQL Lab. PMO yêu cầu bổ sung bộ saved query để:
- Cho phép analyst tự drill-down, khám phá dữ liệu mà không cần viết SQL từ đầu
- Chuẩn hóa các câu hỏi business thành query mẫu (tái sử dụng, audit được)
- Tạo baseline cho báo cáo định kỳ (daily/weekly/monthly ops review)

## Phạm vi dữ liệu

| Nguồn | Schema | Đặc điểm | Dùng cho |
|---|---|---|---|
| Trino (gold) | `hive.nyc_gold.*` | Parquet trên MinIO, 30 tables | Aggregated views, cross-check |
| Trino (mart) | `hive.mart.*` | Views từ dbt, 8 models | Raw/rich data, drill-down |
| Postgres analytics | `public.*` | 30 gold tables đã materialize | Dashboard datasets |

**Quyết định**: Tất cả saved query dùng **Trino** (`hive.mart.*` và `hive.nyc_gold.*`) vì:
- Trino có dữ liệu chi tiết nhất (8-10M trips, có thể GROUP BY theo ý muốn)
- Postgres chỉ có aggregated — phù hợp dashboard, không phù hợp ad-hoc query
- Trino SQL Lab đã được expose qua Superset driver

## Danh sách Saved Queries (24 câu — 6 nhóm)

### 📊 Nhóm 1: Revenue & Performance (5 queries)

| # | Tên | Mô tả | Database | Query tóm tắt |
|---|---|---|---|---|
| SQ-01 | Daily Revenue Summary | Tổng doanh thu + trip count theo ngày, 30 ngày gần nhất | Trino | `SELECT pickup_date, COUNT(*) AS trips, SUM(total_amount) AS revenue FROM hive.mart.fact_trips WHERE pickup_date >= current_date - INTERVAL '30' DAY GROUP BY pickup_date ORDER BY pickup_date DESC` |
| SQ-02 | Top 10 Revenue Zones | Top 10 pickup zone theo tổng doanh thu | Trino | `SELECT pickup_zone, pickup_borough, COUNT(*) AS trips, SUM(total_amount) AS revenue, AVG(tip_amount / NULLIF(total_amount,0)) AS tip_rate FROM hive.mart.fact_trips WHERE pickup_zone IS NOT NULL GROUP BY pickup_zone, pickup_borough ORDER BY revenue DESC LIMIT 10` |
| SQ-03 | Revenue by Hour & Day of Week | Heatmap doanh thu: giờ × thứ trong tuần | Trino | `SELECT pickup_dow, pickup_hour, COUNT(*) AS trips, SUM(total_amount) AS revenue FROM hive.mart.fact_trips GROUP BY pickup_dow, pickup_hour ORDER BY pickup_dow, pickup_hour` |
| SQ-04 | Average Fare by Distance Bucket | Phân tích fare theo khoảng cách | Trino | `SELECT CASE WHEN trip_distance < 1 THEN '<1mi' WHEN trip_distance < 3 THEN '1-3mi' WHEN trip_distance < 10 THEN '3-10mi' WHEN trip_distance < 20 THEN '10-20mi' ELSE '20+mi' END AS dist_bucket, COUNT(*) AS trips, AVG(fare_amount) AS avg_fare, AVG(total_amount) AS avg_total FROM hive.mart.fact_trips GROUP BY 1 ORDER BY MIN(trip_distance)` |
| SQ-05 | Monthly Revenue Comparison | So sánh doanh thu giữa các tháng (MoM growth) | Trino | `SELECT pickup_year, pickup_month, COUNT(*) AS trips, SUM(total_amount) AS revenue, SUM(tip_amount) AS tips, AVG(total_amount) AS avg_per_trip FROM hive.mart.fact_trips GROUP BY pickup_year, pickup_month ORDER BY pickup_year, pickup_month` |

### 🛣️ Nhóm 2: Route Analysis (5 queries)

| # | Tên | Mô tả | Database | Query tóm tắt |
|---|---|---|---|---|
| SQ-06 | Top 20 Popular Routes | Cặp pickup-dropoff phổ biến nhất | Trino | `SELECT pickup_zone, dropoff_zone, COUNT(*) AS trips, AVG(total_amount) AS avg_fare FROM hive.mart.fact_trips WHERE pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL GROUP BY pickup_zone, dropoff_zone ORDER BY trips DESC LIMIT 20` |
| SQ-07 | Borough-to-Borough Flow Matrix | Ma trận luồng đi lại giữa các borough | Trino | `SELECT pickup_borough, dropoff_borough, COUNT(*) AS trips, SUM(total_amount) AS revenue, AVG(trip_distance) AS avg_dist FROM hive.mart.fact_trips WHERE pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL GROUP BY pickup_borough, dropoff_borough ORDER BY trips DESC` |
| SQ-08 | Airport Trip Analysis | Phân tích trip đến/từ JFK, LaGuardia, Newark | Trino | `SELECT CASE WHEN pickup_zone LIKE '%JFK%' THEN 'JFK' WHEN pickup_zone LIKE '%LaGuardia%' THEN 'LGA' WHEN pickup_zone LIKE '%Newark%' THEN 'EWR' ELSE NULL END AS airport, CASE WHEN pickup_zone LIKE '%JFK%' OR pickup_zone LIKE '%LaGuardia%' OR pickup_zone LIKE '%Newark%' THEN 'From Airport' WHEN dropoff_zone LIKE '%JFK%' OR dropoff_zone LIKE '%LaGuardia%' OR dropoff_zone LIKE '%Newark%' THEN 'To Airport' END AS direction, COUNT(*) AS trips, SUM(total_amount) AS revenue FROM hive.mart.fact_trips WHERE (pickup_zone LIKE '%JFK%' OR pickup_zone LIKE '%LaGuardia%' OR pickup_zone LIKE '%Newark%' OR dropoff_zone LIKE '%JFK%' OR dropoff_zone LIKE '%LaGuardia%' OR dropoff_zone LIKE '%Newark%') GROUP BY 1, 2 ORDER BY trips DESC` |
| SQ-09 | Longest Trips (Top 50) | 50 trip dài nhất (theo distance) | Trino | `SELECT pickup_ts, pickup_zone, dropoff_zone, trip_distance, total_amount, passenger_count FROM hive.mart.fact_trips ORDER BY trip_distance DESC LIMIT 50` |
| SQ-10 | Intra-Borough vs Inter-Borough | Tỉ lệ trip nội borough / liên borough | Trino | `SELECT CASE WHEN pickup_borough = dropoff_borough THEN 'Intra-Borough' ELSE 'Inter-Borough' END AS trip_type, COUNT(*) AS trips, AVG(trip_distance) AS avg_dist, AVG(total_amount) AS avg_total FROM hive.mart.fact_trips WHERE pickup_borough IS NOT NULL AND dropoff_borough IS NOT NULL GROUP BY 1` |

### 💳 Nhóm 3: Payment & Fare Analysis (4 queries)

| # | Tên | Mô tả | Database | Query tóm tắt |
|---|---|---|---|---|
| SQ-11 | Payment Type Breakdown | Phân bố phương thức thanh toán | Trino | `SELECT payment_type, COUNT(*) AS trips, SUM(total_amount) AS revenue, AVG(tip_amount) AS avg_tip, AVG(tip_amount / NULLIF(total_amount,0)) AS tip_rate FROM hive.mart.fact_trips GROUP BY payment_type ORDER BY trips DESC` |
| SQ-12 | Credit Card vs Cash — Tip Comparison | So sánh tipping giữa credit card và cash | Trino | `SELECT CASE payment_type WHEN 1 THEN 'Credit Card' WHEN 2 THEN 'Cash' ELSE 'Other' END AS payment, COUNT(*) AS trips, AVG(tip_amount) AS avg_tip, AVG(tip_amount / NULLIF(fare_amount,0)) AS tip_pct_of_fare FROM hive.mart.fact_trips WHERE payment_type IN (1,2) GROUP BY 1` |
| SQ-13 | High-Value Trips ($100+) | Trip có total > $100 — breakdown | Trino | `SELECT pickup_date, pickup_zone, dropoff_zone, total_amount, fare_amount, tip_amount, trip_distance FROM hive.mart.fact_trips WHERE total_amount >= 100 ORDER BY total_amount DESC` |
| SQ-14 | Fare Components Breakdown | Cấu phần giá vé: fare + tolls + surcharge + tips | Trino | `SELECT pickup_date, COUNT(*) AS trips, SUM(fare_amount) AS fare, SUM(extra) AS extra, SUM(mta_tax) AS mta_tax, SUM(tip_amount) AS tips, SUM(tolls_amount) AS tolls, SUM(improvement_surcharge) AS impr_surcharge, SUM(total_amount) AS total FROM hive.mart.fact_trips GROUP BY pickup_date ORDER BY pickup_date` |

### 👥 Nhóm 4: Passenger & Trip Profile (4 queries)

| # | Tên | Mô tả | Database | Query tóm tắt |
|---|---|---|---|---|
| SQ-15 | Passenger Count Distribution | Phân bố số hành khách mỗi trip | Trino | `SELECT passenger_count, COUNT(*) AS trips, AVG(total_amount) AS avg_total, AVG(trip_distance) AS avg_dist FROM hive.mart.fact_trips GROUP BY passenger_count ORDER BY passenger_count` |
| SQ-16 | Trip Duration Analysis | Phân tích thời gian trip (phân vị) | Trino | `SELECT CASE WHEN trip_duration_sec < 300 THEN '<5min' WHEN trip_duration_sec < 600 THEN '5-10min' WHEN trip_duration_sec < 1800 THEN '10-30min' WHEN trip_duration_sec < 3600 THEN '30-60min' ELSE '60+min' END AS duration_bucket, COUNT(*) AS trips, AVG(trip_distance) AS avg_dist, AVG(total_amount) AS avg_total FROM hive.mart.fact_trips GROUP BY 1 ORDER BY MIN(trip_duration_sec)` |
| SQ-17 | Rush Hour vs Off-Peak | So sánh trip trong/ngoài giờ cao điểm | Trino | `SELECT CASE WHEN pickup_hour BETWEEN 7 AND 9 THEN 'Morning Rush' WHEN pickup_hour BETWEEN 16 AND 19 THEN 'Evening Rush' WHEN pickup_hour BETWEEN 10 AND 15 THEN 'Midday' WHEN pickup_hour BETWEEN 20 AND 23 THEN 'Evening' ELSE 'Night' END AS period, COUNT(*) AS trips, AVG(trip_duration_sec)/60 AS avg_duration_min, AVG(total_amount) AS avg_total FROM hive.mart.fact_trips GROUP BY 1 ORDER BY MIN(pickup_hour)` |
| SQ-18 | Vendor Performance Comparison | So sánh 2 vendor (Creative Mobile vs VeriFone) | Trino | `SELECT vendor_id, COUNT(*) AS trips, SUM(total_amount) AS revenue, AVG(total_amount) AS avg_revenue, AVG(trip_distance) AS avg_dist FROM hive.mart.fact_trips GROUP BY vendor_id ORDER BY trips DESC` |

### 🔍 Nhóm 5: Data Quality & Operations (4 queries)

| # | Tên | Mô tả | Database | Query tóm tắt |
|---|---|---|---|---|
| SQ-19 | Validation Error Summary | Tổng quan lỗi validation — theo reason | Trino | `SELECT validation_error, SUM(error_count) AS total_errors FROM hive.mart.fact_invalid_trips GROUP BY validation_error ORDER BY total_errors DESC` |
| SQ-20 | Invalid Trip Rate by Day | Tỉ lệ trip lỗi / tổng trip theo ngày | Trino | `WITH valid AS (SELECT pickup_date, COUNT(*) AS valid_trips FROM hive.mart.fact_trips GROUP BY pickup_date), invalid AS (SELECT pickup_date, SUM(error_count) AS invalid_trips FROM hive.mart.fact_invalid_trips GROUP BY pickup_date) SELECT v.pickup_date, v.valid_trips, COALESCE(i.invalid_trips, 0) AS invalid_trips, ROUND(COALESCE(i.invalid_trips,0) * 100.0 / v.valid_trips, 2) AS error_rate_pct FROM valid v LEFT JOIN invalid i ON v.pickup_date = i.pickup_date ORDER BY v.pickup_date` |
| SQ-21 | Row Count Reconciliation | So sánh row count giữa Trino silver, mart, và Postgres gold | Trino | `SELECT 'silver' AS layer, COUNT(*) AS rows FROM hive.nyc.trips UNION ALL SELECT 'mart', COUNT(*) FROM hive.mart.fact_trips UNION ALL SELECT 'gold', COUNT(*) FROM hive.nyc_gold.fact_trips_daily` |
| SQ-22 | Zone Coverage Check | Zone nào có trong dim_zone nhưng chưa từng có trip? | Trino | `SELECT z.zone, z.borough FROM hive.mart.dim_zone z LEFT JOIN (SELECT DISTINCT pickup_zone AS zone FROM hive.mart.fact_trips UNION SELECT DISTINCT dropoff_zone FROM hive.mart.fact_trips) t ON z.zone = t.zone WHERE t.zone IS NULL ORDER BY z.borough, z.zone` |

### 📈 Nhóm 6: Trend & Pattern Queries (2 queries)

| # | Tên | Mô tả | Database | Query tóm tắt |
|---|---|---|---|---|
| SQ-23 | Weekday vs Weekend Patterns | So sánh trip pattern ngày thường và cuối tuần | Trino | `SELECT CASE WHEN pickup_dow IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type, pickup_hour, COUNT(*) AS trips, SUM(total_amount) AS revenue FROM hive.mart.fact_trips GROUP BY 1, pickup_hour ORDER BY 1, pickup_hour` |
| SQ-24 | Daily KPIs with MoM Comparison | Bảng KPI hàng ngày có so sánh tuần trước | Trino | `WITH daily AS (SELECT pickup_date, COUNT(*) AS trips, SUM(total_amount) AS revenue, AVG(tip_amount/NULLIF(total_amount,0))*100 AS tip_rate_pct FROM hive.mart.fact_trips GROUP BY pickup_date) SELECT d.pickup_date, d.trips, d.revenue, ROUND(d.tip_rate_pct,1) AS tip_rate_pct, LAG(d.trips, 7) OVER (ORDER BY d.pickup_date) AS trips_prev_week, ROUND((d.trips - LAG(d.trips,7) OVER (ORDER BY d.pickup_date)) * 100.0 / NULLIF(LAG(d.trips,7) OVER (ORDER BY d.pickup_date),0), 1) AS trips_wow_change_pct FROM daily d ORDER BY d.pickup_date DESC` |

## Trạng thái: ✅ Implemented

Script: `scripts/superset_saved_queries.py`
DAG integration: `nyc_e2e_pipeline` + `nyc_analytics_refresh` (runs after `superset_bootstrap`)

---

## Kế hoạch triển khai

### Phase 1: Core Business (8 queries — ưu tiên P0)

Triển khai ngay nhóm Revenue + Route:
- SQ-01, SQ-02, SQ-05, SQ-06, SQ-07, SQ-08, SQ-11, SQ-20

### Phase 2: Operations & QA (8 queries — P1)

- SQ-03, SQ-04, SQ-12, SQ-15, SQ-16, SQ-17, SQ-19, SQ-22

### Phase 3: Complete (8 queries — P2)

- SQ-09, SQ-10, SQ-13, SQ-14, SQ-18, SQ-21, SQ-23, SQ-24

### Cách triển khai

Thêm script `scripts/superset_saved_queries.py`:
- Dùng Superset REST API (`POST /api/v1/saved_query/`)
- Idempotent: skip nếu `label` đã tồn tại
- Database target: Trino connection (tạo trước nếu chưa có)
- Schema: `hive.mart` (default), một số query dùng `hive.nyc_gold`

Script sẽ chạy trong Airflow DAG task hoặc manual `python scripts/superset_saved_queries.py`.

### Cấu trúc API payload mỗi saved query

```json
{
  "db_id": <trino_db_id>,
  "schema": "hive.mart",
  "label": "SQ-01: Daily Revenue Summary",
  "description": "Tổng doanh thu + trip count theo ngày, 30 ngày gần nhất",
  "sql": "SELECT pickup_date, COUNT(*) AS trips, ...",
  "extra_json": "{\"group\":\"Revenue\"}"
}
```

### Tích hợp với pipeline hiện tại

- Airflow task `superset_saved_queries` trong DAG `nyc_e2e_pipeline`
- Chạy sau `superset_bootstrap` (sau khi dataset và dashboard đã sẵn sàng)
- Idempotent: chạy lại không tạo duplicate, chỉ update nếu sql thay đổi
