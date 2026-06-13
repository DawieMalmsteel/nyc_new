# Kế hoạch: Gold Analytics Layer — Data Mart cho Business

## Vision

Xây dựng **Gold Layer hoàn chỉnh** với các dataset phân tích kinh doanh, xuất Parquet vào MinIO sẵn sàng cho:
- BI tools (Superset, Power BI, Tableau)
- ML models (forecast, anomaly detection)
- Data sharing với team khác
- Ad-hoc analytics bằng Trino/Spark

## Kiến trúc

```
Silver (MinIO) ──→ Trino ──→ dbt (views) ──→ Trino CTAS ──→ Gold (MinIO)
                              ↑                               ↑
                        15 models                     ~30 gold datasets
                                                      + data quality checks
                                                      + metadata tracking
                                                      + zone-level KPI
                                                      + OD matrix
```

## Gold Datasets

### 1. Fact Tables (chi tiết giao dịch)

| Dataset | Grain | Key Columns | Ghi chú |
|---------|-------|-------------|---------|
| `fact_trips` | Mỗi chuyến xe | trip_id, vendor_id, pickup_ts, dropoff_ts, fare, distance, zones, tip_rate, trip_duration | Enriched từ silver |
| `fact_trips_enriched` | Mỗi chuyến xe + flags | fact_trips + is_airport_trip, trip_time_category, inferred_purpose, zone_volume_tier | Bổ sung cột phân loại cho phân tích chuyên sâu |
| `fact_trips_daily` | 1 row / ngày | pickup_date, trip_count, total_revenue, avg_fare, avg_tip%, avg_distance, total_passengers | Tổng hợp ngày |
| `fact_trips_hourly` | 1 row / giờ | pickup_date, pickup_hour, trip_count, revenue, avg_wait_time | Phân tích peak giờ |
| `fact_trips_hourly_zone` | 1 row / zone / giờ | pickup_zone, borough, pickup_date, pickup_hour, trip_count, total_revenue, avg_fare, dropoff_count | Phân tích peak giờ theo từng zone — quan trọng cho zone nhỏ như POM |
| `fact_trips_borough` | 1 row / pickup_borough / ngày | pickup_date, borough, trip_count, revenue, avg_distance | Phân tích theo quận |

### 2. Dimension Tables (thông tin tham chiếu)

| Dataset | Mô tả |
|---------|-------|
| `dim_zone` | Zone info: location_id, borough, zone, service_zone |
| `dim_zone_grouped` | Zone với phân nhóm volume: location_id, zone, borough, trip_volume_tier (High/Medium/Low/VeryLow), group_name (ví dụ Queens_Residential_South) — xử lý long-tail, gộp zone nhỏ để phân tích có ý nghĩa thống kê |
| `dim_date` | Date dimension: date, year, month, day, day_of_week, is_weekend, is_holiday, quarter, week_of_year |
| `dim_vendor` | Vendor info: vendor_id, vendor_name (mapping 1=Creative Mobile, 2=VeriFone) |
| `dim_payment_type` | Payment type: code, description (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip) |
| `dim_rate_code` | Rate code: code, description (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride) |

### 3. KPI & Business Metrics (phân tích kinh doanh)

| Dataset | Ý nghĩa | Use case |
|---------|---------|----------|
| `kpi_daily_overview` | Tổng quan KPI mỗi ngày | Dashboard chính |
| | trips, revenue, avg_fare, avg_tip%, | |
| | avg_distance, unique_drivers, utilization_rate | |
| `kpi_weekly_trends` | Xu hướng tuần, so sánh WoW | Báo cáo tuần |
| | trip_count, revenue_growth%, avg_fare_change% | |
| `kpi_monthly_summary` | Tổng kết tháng, YoY comparison | Báo cáo tháng |
| | total_revenue, yoy_growth%, ytd_revenue, | |
| | avg_trip_per_day, market_share_by_vendor | |
| `kpi_borough_comparison` | So sánh doanh thu giữa các quận | Phân tích vùng |
| | borough, trips, revenue, market_share% | |
| `kpi_zone_performance` | **KPI theo từng zone** — zone, borough, pickups, dropoffs, net_flow, avg_fare, avg_tip%, total_revenue, airport_trip_count, airport_trip_pct | Phân tích zone cụ thể (POM, JFK, ...) |
| `kpi_zone_net_flow` | **Cân bằng luồng taxi** — zone, borough, pickups, dropoffs, net_flow, net_flow_ratio, imbalance_score, primary_inflow_source, primary_outflow_dest | Phát hiện zone "thâm hụt" taxi (POM: 4.4x dropoffs > pickups) — hỗ trợ dispatch optimization |
| `kpi_payment_trends` | Xu hướng thanh toán | Phân tích payment |
| | payment_type, trip_count, revenue, avg_tip_by_type | |
| `kpi_vendor_performance` | So sánh vendor | Vendor analysis |
| | vendor_id, trips, revenue, avg_rating, | |
| | on_time_rate, market_share% | |

### 4. Route & Operational Analysis (phân tích vận hành)

| Dataset | Ý nghĩa |
|---------|---------|
| `route_top_pickup_zones` | Top 20 zone có nhiều pickup nhất (theo ngày/giờ) |
| `route_top_dropoff_zones` | Top 20 điểm đến phổ biến |
| `route_popular_routes` | Cặp pickup→dropoff phổ biến nhất, revenue theo route |
| `route_airport_analysis` | Trip đến/từ JFK, LGA, EWR — revenue, distance, tip |
| `route_airport_zone_matrix` | **Ma trận sân bay → zone dân cư** — airport_zone, residential_zone, borough, trips, avg_fare, avg_distance, peak_hour, avg_tip% | Phân tích flow từ sân bay về từng zone (POM có 47% trips từ airport) |
| `route_cross_borough` | Trip liên quận: flow giữa các borough |
| `od_borough_matrix` | **Ma trận xuất xứ-đích (borough)** — pickup_borough, dropoff_borough, trip_count, total_revenue, avg_fare, avg_distance, avg_tip%, pct_of_total | Full OD matrix để phân tích luồng di chuyển liên quận |
| `ops_peak_hours_heatmap` | Heatmap pickup (giờ × thứ trong tuần) |
| | giúp trả lời: "Khi nào cần nhiều tài xế nhất?" |
| `ops_trip_distance_distribution` | Phân phối khoảng cách (bucket: 0-1,1-3,3-5,5-10,10+ miles) |
| `ops_passenger_count_pattern` | Số lượng khách theo giờ/quận |
| `ops_utilization_rate` | Tỷ lệ trip có tip, trip có passenger > 1 |

### 5. Data Quality & Audit (chất lượng dữ liệu)

| Dataset | Ý nghĩa |
|---------|---------|
| `dq_validation_summary` | Tổng quan: valid/invalid rate theo ngày |
| `dq_invalid_by_reason` | Chi tiết lỗi validation (payment_type, distance, nulls...) |
| `dq_row_count_trend` | Số lượng trip theo ngày, phát hiện anomaly |
| `dq_batch_metadata` | Metadata mỗi lần export: timestamp, rows, duration, status |

## Output Format

- **Format**: Parquet (columnar, nén Snappy/ZSTD)
- **Partitioning**: Theo `pickup_year/pickup_month` cho fact tables
- **Location**: `s3a://nyc-gold/{dataset_name}/year={yyyy}/month={mm}/`
- **Mode**: Overwrite toàn bộ mỗi lần chạy (snapshot)

## Thống kê dung lượng dự kiến

| Dataset | Rows (ước) | Size (ước) |
|---------|-----------|-----------|
| `fact_trips` | 5.4M | ~200MB |
| `fact_trips_daily` | ~90 | ~20KB |
| `fact_trips_hourly` | ~2K | ~100KB |
| `fact_trips_borough` | ~360 | ~50KB |
| `dim_date` | 1,095 (3 năm) | ~100KB |
| `kpi_*` | ~100-500 mỗi dataset | ~50KB-1MB |
| `route_*` | ~20-500 mỗi dataset | ~10KB-500KB |
| `dq_*` | ~90-5.4M | ~10KB-50MB |
| `fact_trips_enriched` | 5.4M | ~210MB |
| `fact_trips_hourly_zone` | ~265K (265 zones × ~1K giờ) | ~15MB |
| `kpi_zone_performance` | ~8K (265 zones × ~30 ngày) | ~1MB |
| `kpi_zone_net_flow` | ~265 | ~50KB |
| `route_airport_zone_matrix` | ~800 (3 airport × 265 zones) | ~200KB |
| `od_borough_matrix` | ~64 (8 borough × 8) | ~10KB |
| `dim_zone_grouped` | 265 | ~20KB |
| **Tổng** | | **~350-500MB** |

## Implement

### Script export

Dùng Trino CTAS (`CREATE TABLE AS WITH (external_location=...)`) cho mỗi dataset.

Script `scripts/export_gold_to_minio.py`:
- Định nghĩa tất cả gold datasets trong cấu trúc dữ liệu
- Mỗi dataset có: name, source SQL, location path, partition columns
- Tự động tạo schema `hive.nyc` nếu chưa có
- `DROP + CREATE TABLE` cho mỗi dataset
- Log kết quả từng dataset (rows, duration)
- Exit code non-zero nếu bất kỳ dataset nào fail

### Task trong DAG

```python
gold_export = KubernetesPodOperator(
    name="gold-export",
    image="nyc-pipeline-tools:k8s",
    cmds=["python3", "/opt/project/scripts/export_gold_to_minio.py"],
    ...
)

# Luồng:
trino_bootstrap >> dbt_build >> gold_export >> superset_bootstrap
```

### Cập nhật MinIO

- Thêm bucket `nyc-gold` vào:
  - `terraform/variables.tf` (bucket_names)
  - `charts/.../minio-setup.yaml` (vòng lặp create bucket)

## Câu hỏi Business mà Gold Layer trả lời được

1. **Doanh thu hôm nay bao nhiêu? So với hôm qua?**
2. **Quận nào đem lại doanh thu cao nhất?**
3. **Zone nào đem lại doanh thu cao nhất? Zone nào thâm hụt taxi nhiều nhất?** (kpi_zone_performance, kpi_zone_net_flow)
4. **Giờ nào trong ngày có nhiều trip nhất? Theo từng zone?** (fact_trips_hourly_zone)
5. **Tỷ lệ tip có thay đổi theo quận không? Theo zone?**
6. **Tuyến đường nào phổ biến nhất?**
7. **Bao nhiêu trip từ sân bay về từng zone dân cư?** (route_airport_zone_matrix) — POM có 47% trips từ JFK/LGA
8. **Vendor nào có thị phần lớn nhất?**
9. **Xu hướng thanh toán tiền mặt vs thẻ?**
10. **Ma trận đi lại giữa các quận?** (od_borough_matrix)
11. **Bao nhiêu trip bị invalid và tại sao?**
12. **Chất lượng dữ liệu có cải thiện theo thời gian?**
13. **Dự báo doanh thu tuần sau?**

## Timeline

| Bước | Mô tả | Thời gian |
|------|-------|-----------|
| 1 | Tạo bucket nyc-gold (terraform + minio-setup) | 15 phút |
| 2 | Viết script `export_gold_to_minio.py` với ~30 datasets | 2-3 giờ |
| 2a | Thêm 7 datasets zone-focused + logic net flow + zone grouping | +1 giờ |
| 2b | Thêm cột is_airport_trip, inferred_purpose vào fact_trips_enriched | +30 phút |
| 3 | Thêm task `gold_export` vào 2 DAGs | 15 phút |
| 4 | Deploy + verify | 30 phút |
| 5 | Tạo Superset charts/dashboard từ gold data | 1 giờ |
