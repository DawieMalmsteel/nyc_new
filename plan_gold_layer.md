# Gold Layer Feasibility Assessment

## Data Profile (verified from silver parquet)
| Metric | Value |
|---|---|
| Date range | 2024-01-01 → 2024-03-31 (91 ngày) |
| Total trips | 8,401,312 |
| Vendors | 1=1,949,422 (23%), 2=6,451,890 (77%) — meaningful split |
| Payment types | 1=Credit(7.05M), 2=Cash(1.25M), 3=NoCharge(31K), 4=Dispute(73K) |
| Rate codes | 1=7.98M, 2=288K, 3=20K, 4=1.8K, 5=19K, 6=6, 99=91K |
| Passenger counts | 1-6 |
| Boroughs | 6 (Bronx, Brooklyn, EWR, Manhattan, Queens, Staten Island) |
| Distinct zones | 252 |
| Tip rate | 80% trips have tip>0 |
| Outliers | trip>500mi=28, total>$500=21, tip>$100=49 (0.0003% — negligible) |

## Data Limitations (ảnh hưởng đến implement)

| # | Thiếu | Ảnh hưởng |
|---|---|---|
| L1 | **Không có driver_id** | `driver_earnings` → dùng trip-level avg. Không track được driver cá nhân. |
| L2 | **Không có rider_id** | `customer_segments`, `retention` → zone-level proxy, không phải user thật. |
| L3 | **Chỉ 3 tháng** | MoM growth = 2 điểm dữ liệu. Không có seasonality, year-over-year. |
| L4 | **Không có cost data** | `unit_economics` chỉ có revenue, không có margin/profit. |
| L5 | **Không có weather/event** | Không phân tích được yếu tố ngoại cảnh. |
| L6 | **Payment type 5,6 = 0 dòng** | Chỉ phân tích được 4 loại thanh toán. |
| L7 | **Rate code 99 tồn tại** | 91K trip có rate_code=99 — cần clean hoặc document. |
| L8 | **Outlier trip_distance >500mi, total>$500** | 28+21 trip siêu nhỏ — filter trong gold layer. |

---

## Feasibility by Table

### 📢 MARKETING (8 bảng)

| # | Bảng | Khả thi? | Ghi chú |
|---|---|---|---|
| M1 | `gold_customer_segments` | ✅ | Segments = spend × distance × frequency. Zone-level (L2). |
| M2 | `gold_customer_journey` | ✅ | Top OD pairs theo giờ. Có đủ cột. |
| M3 | `gold_payment_behavior` | ✅ | Trend payment_type theo thời gian. 4 loại (L7). |
| M4 | `gold_zone_demographics` | ✅ | Classify zone: residential (sáng pickup), business (sáng dropoff), mixed, nightlife. |
| M5 | `gold_retention_proxy` | ⚠️ | Zone-level: % trip quay lại zone cũ. Không phải user retention (L2). |
| M6 | `gold_campaign_target_zones` | ✅ | Composite score: growth + spend + passenger_count >1. |
| M7 | `gold_weekend_vs_weekday` | ✅ | So sánh trực tiếp từ pickup_dow. |
| M8 | `gold_rider_tipping_culture` | ✅ | Tip rate × zone × hour. Có đủ cột. |

### 💼 SALES (8 bảng)

| # | Bảng | Khả thi? | Ghi chú |
|---|---|---|---|
| S1 | `gold_driver_earnings` | ⚠️ | Trip-level avg fare+trip/giờ. Không phải per-driver (L1). Đổi tên → `gold_trip_unit_economics`. |
| S2 | `gold_driver_demand_forecast` | ⚠️ | Supply proxy = vendor count (chỉ 2). Quá mỏng (L4). Đổi → `gold_zone_demand_heatmap` (chỉ demand side). |
| S3 | `gold_fleet_partnership` | ✅ | Top zone theo revenue + growth. OK. |
| S4 | `gold_airport_corporate` | ✅ | EWR có trong data. JFK/LGA nếu zone lookup có. |
| S5 | `gold_vendor_battlecard` | ✅ | 2 vendor đủ meaningful: 23% vs 77% market share. |
| S6 | `gold_growth_metrics` | ⚠️ | WoW có ~13 điểm, MoM chỉ 2 điểm (L3). Bỏ MoM, giữ WoW. |
| S7 | `gold_unit_economics` | ✅ | Fare, duration, distance, fare/km. Revenue-only (L5). |
| S8 | `gold_zone_whitepaper` | ✅ | Tổng hợp per zone: revenue, trips, growth, peak, fare, tip. |

### 👔 CEO/BOARD (8 bảng)

| # | Bảng | Khả thi? | Ghi chú |
|---|---|---|---|
| C1-C3 | `gold_executive_daily/weekly/monthly` | ✅ | Daily (91 điểm), weekly (13), monthly (3 — mỏng). Gộp C2+C3 → `gold_executive_weekly`. |
| C4 | `gold_revenue_waterfall` | ✅ | fare + tip + tolls + surcharge breakdown. Có đủ cột. |
| C5 | `gold_topline_kpi` | ⚠️ | "burn proxy" không có cost (L5). Bỏ, giữ GMV, AOV, DAU. |
| C6 | `gold_risk_dashboard` | ✅ | Có sẵn anomaly_flag + freshness + validation. |
| C7 | `gold_hourly_pulse` | ✅ | Pivot 24×7 heatmap. |
| C8 | `gold_competitive_landscape` | ✅ | Trùng với S5 + M3. Gộp vào S5. |

### 🛡️ SHARED (3 bảng)

| # | Bảng | Khả thi? | Ghi chú |
|---|---|---|---|
| Q1-Q3 | Quality tables | ✅ | Đã có dbt model. |

---

## Final Viable Gold Layer: 20 bảng

Sau khi cắt bảng không khả thi + gộp bảng trùng:

| # | Bảng | Team | Priority |
|---|---|---|---|
| 1 | `gold_customer_segments` | Marketing | P1 |
| 2 | `gold_customer_journey` | Marketing | P1 |
| 3 | `gold_payment_behavior` | Marketing | P1 |
| 4 | `gold_zone_demographics` | Marketing | P2 |
| 5 | `gold_campaign_target_zones` | Marketing | P2 |
| 6 | `gold_weekend_vs_weekday` | Marketing | P2 |
| 7 | `gold_tipping_culture` | Marketing | P1 |
| 8 | `gold_trip_unit_economics` | Sales | P1 |
| 9 | `gold_zone_demand_heatmap` | Sales | P1 |
| 10 | `gold_fleet_partnership` | Sales | P2 |
| 11 | `gold_airport_corporate` | Sales | P2 |
| 12 | `gold_vendor_battlecard` | Sales | P1 |
| 13 | `gold_growth_metrics` | Sales | P1 |
| 14 | `gold_zone_whitepaper` | Sales | P2 |
| 15 | `gold_executive_daily` | CEO | P1 |
| 16 | `gold_executive_weekly` | CEO | P1 |
| 17 | `gold_revenue_waterfall` | CEO | P1 |
| 18 | `gold_hourly_pulse` | CEO | P1 |
| 19 | `gold_risk_dashboard` | CEO | P1 |
| 20 | `gold_topline_kpi` | CEO | P2 |

### Shared quality (đã có, giữ nguyên)
| Q1 | `gold_anomaly_daily` | Shared | Done |
| Q2 | `gold_freshness` | Shared | Done |
| Q3 | `gold_validation_summary` | Shared | Done |

---

## Design Notes

**P1 (13 bảng)** — Implement ngay. Core business value, data đủ.

**P2 (7 bảng)** — Có thể làm, nhưng insight mỏng do data limitation. Giữ plan, làm sau.

**Đã cắt (7 bảng)**:
- `gold_retention_proxy` → user-level không khả thi (L2)
- `gold_driver_earnings` → đổi thành trip_unit_economics (L1)
- `gold_driver_demand_forecast` → đổi thành zone_demand_heatmap (L4)
- `gold_unit_economics` → gộp vào trip_unit_economics
- `gold_executive_monthly` → gộp vào weekly (L3)
- `gold_topline_kpi.burn_proxy` → bỏ do L5
- `gold_competitive_landscape` → gộp vào vendor_battlecard
