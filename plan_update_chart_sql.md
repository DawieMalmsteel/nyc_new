# Plan: Upgrade Superset Chart Visuals & Metrics

## Vấn đề hiện tại

### 1. Metric labels là raw column names
Hiển thị `trip_count`, `total_revenue`, `pickup_trip_count`, `net_flow` thay vì "Trip Count", "Total Revenue", "Zone Volume", "Net Flow". Không ai muốn đọc `avg_tip_pct` trên chart.

### 2. Charts quá đơn — mỗi chart 1 metric
Hầu hết chart chỉ show 1 metric (thường là `trip_count`), bỏ qua các cột giàu thông tin khác có sẵn trong gold tables:
- `kpi_daily_overview` có 9 cột: `trips, revenue, avg_fare, avg_tip, avg_tip_pct, avg_distance, unique_vendors, utilization_rate`
- `od_borough_matrix` có 8 cột: `trip_count, total_revenue, avg_fare, avg_distance, avg_tip, pct_of_total`
- `kpi_monthly_summary` có: `trip_count, total_revenue, avg_fare, avg_distance, avg_trip_per_day, prev_month_revenue, mom_growth_pct`

### 3. Table charts format kém
- "Batch Metadata" chỉ show raw columns không có format
- "Zone Directory" bị skip do dataset lỗi

### 4. Big number KPI thiếu context
"All-Time Trip Count" và "Total Revenue" chỉ show số, không có subtitle về period hoặc trend indicator

---

## Plan cải thiện

### A. Sửa `_m()` helper — label đẹp hơn

```python
# Before
def _m(col_name: str, aggregate: str = "SUM") -> dict:
    return {"aggregate": aggregate, "column": {"column_name": col_name},
            "expressionType": "SIMPLE", "label": col_name}

# After
def _m(col_name: str, aggregate: str = "SUM", label: str = None) -> dict:
    if label is None:
        label = col_name.replace("_", " ").title()
    return {"aggregate": aggregate, "column": {"column_name": col_name},
            "expressionType": "SIMPLE", "label": label}
```

### B. Upgrade từng chart — thêm metric thứ 2, label đẹp

#### Nhóm KPIs (big_number)
| Chart | Hiện tại | Mới |
|---|---|---|
| All-Time Trip Count | `label: trips` | `label: "Total Trips"` + subtitle "Jan–Mar 2024" |
| Total Revenue | `label: revenue` | `label: "Total Revenue"` + `number_format: "$,.0f"` |

#### Nhóm Revenue & Trends (echarts, dist_bar)
| Chart | Hiện tại | Mới |
|---|---|---|
| Daily Revenue | 1 metric: `revenue` | `label: "Revenue"` + thêm dual axis `"Trip Count"` |
| Daily Trips | 1 metric: `trips` | `label: "Trips"` + thêm `"Avg Fare"` |
| Weekly Trip Trends | `label: trip_count` | `label: "Trips"` + `"Trip Growth %"` từ `trip_growth_pct` |
| Monthly Summary | `label: total_revenue` | `label: "Revenue"` + `"MoM Growth %"` từ `mom_growth_pct` |
| Borough Trips Over Time | `label: trip_count` | `label: "Trips"` (stacked borough) |
| Hourly Trip Pattern | `label: trip_count` | `label: "Trips"` + thêm `"Avg Fare"` |

#### Nhóm Revenue Breakdown (pie — giữ nguyên 1 metric, sửa label)
| Chart | Hiện tại | Mới |
|---|---|---|
| Borough Market Share | `label: revenue` | `label: "Revenue"` |
| Payment Types | `label: trip_count` | `label: "Trips"` |
| Vendor Market Share | `label: trips` | `label: "Trips"` |
| Airport Direction | `label: trip_count` | `label: "Trips"` |

#### Nhóm Routes & Zones (dist_bar)
| Chart | Hiện tại | Mới |
|---|---|---|
| Top Pickup Zones | `label: trip_count` | `label: "Trips"` + thêm `"Revenue"` |
| Top Dropoff Zones | `label: trip_count` | `label: "Trips"` + thêm `"Revenue"` |
| Popular Routes | `label: trip_count` | `label: "Trips"` + `"Avg Fare"` |
| Airport Trip Stats | `label: trip_count` | `label: "Trips"` |
| Cross-Borough Routes | `label: trip_count` | `label: "Trips"` |
| Airport × Zone | `label: trips` | `label: "Trips"` |
| Borough OD Flow | `label: trip_count` | `label: "Trips"` + thêm `"% of Total"` từ `pct_of_total` |
| Zone Performance | `label: pickups` | `label: "Pickups"` |
| Zone Net Flow | `label: net_flow` | `label: "Net Flow"` |
| Zone Trip Volume | `label: trip_count` | `label: "Trips"` |
| Zone Groups (Volume) | `label: pickup_trip_count` | `label: "Pickup Trips"` |

#### Nhóm Trip Profile (dist_bar)
| Chart | Hiện tại | Mới |
|---|---|---|
| Trip Distance Dist. | `label: trip_count` | `label: "Trips"` + thêm `"Avg Fare"` từ `avg_fare` |
| Passenger Count Pat. | `label: trip_count` | `label: "Trips"` |
| Peak Hours | `label: trip_count` | `label: "Trips"` + thêm `"Avg Fare"` |

#### Nhóm Data Quality
| Chart | Hiện tại | Mới |
|---|---|---|
| Quality Checks | `label: total_trips` | `label: "Total Trips"` — giữ nguyên (dq table đặc thù) |
| Row Count Trend | `label: trip_count` | `label: "Trip Count"` |
| Batch Metadata | table, raw count metric | Table với `column_config` — format date, file paths |
| Zone Directory | SKIP (lỗi dataset) | Fix: tạo dataset `dim_zone` đúng cách, table với columns: zone, borough, service_zone |

### C. Table charts — column_config

"Batch Metadata" (1 row) → show key columns: `batch_id`, `processed_at`, `file_count`, `total_rows`, `duration_sec` với format phù hợp.

"Zone Directory" (265 rows) → fix dataset `dim_zone`, rename columns: `zone` → "Zone", `borough` → "Borough", `service_zone` → "Service Zone".

### D. Add chart descriptions to every chart

Mỗi chart CHART_DEFS đã có description — giữ nguyên và bổ sung thêm context về period/unit.

---

## Implementation

Sửa trong `scripts/superset_bootstrap.py`:

1. **`_m()` helper** — thêm optional `label` parameter, auto-title-case
2. **CHART_DEFS** — thay label raw → human-readable, thêm dual metrics nơi có ý nghĩa
3. **Table charts** — thêm `column_config` cho formatted display
4. **Big number** — thêm `subheader` hiển thị period context

File: `scripts/superset_bootstrap.py` (45 CHART_DEFS lines + _m() helper cần sửa)

Không thay đổi logic khác — idempotent, DAG integration, API patterns giữ nguyên.
