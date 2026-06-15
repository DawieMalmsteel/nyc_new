#!/usr/bin/env python3
"""
superset_bootstrap.py — Register Trino DB, gold datasets, charts, and dashboard.

Idempotent: skips resources that already exist. Uses REST API.
Registers all 34 gold tables from hive.nyc_gold as Superset datasets,
plus key mart tables for backward compatibility.

Superset 4.0 note: charts are linked to dashboards via the "dashboards" field
on chart create/update, NOT via POST /dashboard/{id}/charts (removed in 4.0).
"""
import json
import os
import sys
import urllib.request

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088") + "/api/v1"
TRINO_URI = os.environ.get(
    "TRINO_URI",
    "trino://analytics@trino-coordinator:8080/hive"
)

# ── Gold tables grouped by category ──
GOLD_TABLES = [
    # Fact tables (aggregates only — raw 5.4M-row tables via Trino)
    # note: fact_trips_daily not yet created by gold_export
    ("nyc_gold", "fact_trips_hourly"),
    ("nyc_gold", "fact_trips_hourly_zone"),
    ("nyc_gold", "fact_trips_borough"),
    # Dimension tables
    ("nyc_gold", "dim_zone"),
    ("nyc_gold", "dim_zone_grouped"),
    ("nyc_gold", "dim_date"),
    ("nyc_gold", "dim_vendor"),
    ("nyc_gold", "dim_payment_type"),
    ("nyc_gold", "dim_rate_code"),
    # KPI & Business Metrics
    ("nyc_gold", "kpi_daily_overview"),
    ("nyc_gold", "kpi_weekly_trends"),
    ("nyc_gold", "kpi_monthly_summary"),
    ("nyc_gold", "kpi_borough_comparison"),
    ("nyc_gold", "kpi_zone_performance"),
    ("nyc_gold", "kpi_zone_net_flow"),
    ("nyc_gold", "kpi_payment_trends"),
    ("nyc_gold", "kpi_vendor_performance"),
    # Route & Operational
    ("nyc_gold", "route_top_pickup_zones"),
    ("nyc_gold", "route_top_dropoff_zones"),
    ("nyc_gold", "route_popular_routes"),
    ("nyc_gold", "route_airport_analysis"),
    ("nyc_gold", "route_airport_zone_matrix"),
    ("nyc_gold", "route_cross_borough"),
    ("nyc_gold", "od_borough_matrix"),
    ("nyc_gold", "ops_peak_hours_heatmap"),
    ("nyc_gold", "ops_trip_distance_distribution"),
    ("nyc_gold", "ops_passenger_count_pattern"),
    ("nyc_gold", "ops_utilization_rate"),
    # Data Quality
    ("nyc_gold", "dq_validation_summary"),
    ("nyc_gold", "dq_invalid_by_reason"),
    ("nyc_gold", "dq_row_count_trend"),
    ("nyc_gold", "dq_batch_metadata"),
]

# ── Helper: adhoc metric (Superset 4.0 columns need explicit aggregate) ──
def _m(col_name: str, aggregate: str = "SUM") -> dict:
    return {"aggregate": aggregate, "column": {"column_name": col_name},
            "expressionType": "SIMPLE", "label": col_name}


# ── Chart definitions: (name, viz_type, datasource_key, params) ──
# Timeseries/pie charts: metrics must be adhoc objects (columns are not metrics).
# Table charts: omit metrics — columns are auto-detected.
# ── Table chart defaults: all_columns shows all columns ──
_T = {"all_columns": [], "row_limit": 1000, "time_range": "No filter"}
_T100 = {"all_columns": [], "row_limit": 100, "time_range": "No filter"}
_T500 = {"all_columns": [], "row_limit": 500, "time_range": "No filter"}

CHART_DEFS = [
    # ── FACT ──
    ("Hourly Trip Pattern", "echarts_timeseries_bar", "nyc_gold.fact_trips_hourly",
     {"metrics": [_m("trip_count")], "groupby": ["pickup_hour"],
      "granularity_sqla": "pickup_date", "time_range": "No filter"}),
    ("Hourly Zone Detail", "table", "nyc_gold.fact_trips_hourly_zone", _T),
    ("Borough Trip Summary", "table", "nyc_gold.fact_trips_borough", _T),

    # ── DIM ──
    ("Zone Directory", "table", "nyc_gold.dim_zone", _T500),
    ("Zone Groups", "table", "nyc_gold.dim_zone_grouped", _T500),

    # ── KPI ──
    ("Daily Revenue (KPI)", "echarts_timeseries_bar", "nyc_gold.kpi_daily_overview",
     {"metrics": [_m("revenue")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Daily Trips (KPI)", "echarts_timeseries_line", "nyc_gold.kpi_daily_overview",
     {"metrics": [_m("trips")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Daily Utilization", "echarts_timeseries_line", "nyc_gold.kpi_daily_overview",
     {"metrics": [_m("utilization_rate", "AVG")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Weekly Trip Trends", "echarts_timeseries_bar", "nyc_gold.kpi_weekly_trends",
     {"metrics": [_m("trip_count")], "granularity_sqla": "week_start",
      "time_range": "No filter"}),
    ("Weekly Growth Rate", "echarts_timeseries_line", "nyc_gold.kpi_weekly_trends",
     {"metrics": [_m("trip_growth_pct", "AVG"), _m("revenue_growth_pct", "AVG")],
      "granularity_sqla": "week_start", "time_range": "No filter"}),
    ("Monthly Revenue Growth", "table", "nyc_gold.kpi_monthly_summary", _T100),
    ("Borough Market Share", "pie", "nyc_gold.kpi_borough_comparison",
     {"metrics": [_m("revenue")], "groupby": ["pickup_borough"],
      "time_range": "No filter"}),
    ("Zone Performance", "table", "nyc_gold.kpi_zone_performance", _T),
    ("Zone Net Flow", "table", "nyc_gold.kpi_zone_net_flow", _T),
    ("Payment Types", "pie", "nyc_gold.kpi_payment_trends",
     {"metrics": [_m("trip_count")], "groupby": ["payment_type"],
      "time_range": "No filter"}),
    ("Vendor Market Share", "pie", "nyc_gold.kpi_vendor_performance",
     {"metrics": [_m("trips")], "groupby": ["vendor_name"],
      "time_range": "No filter"}),

    # ── ROUTE ──
    ("Top Pickup Zones", "table", "nyc_gold.route_top_pickup_zones", _T100),
    ("Top Dropoff Zones", "table", "nyc_gold.route_top_dropoff_zones", _T100),
    ("Popular Routes", "table", "nyc_gold.route_popular_routes", _T100),
    ("Airport Trip Analysis", "table", "nyc_gold.route_airport_analysis", _T100),
    ("Airport Zone Matrix", "table", "nyc_gold.route_airport_zone_matrix", _T),
    ("Cross-Borough Routes", "table", "nyc_gold.route_cross_borough", _T100),
    ("Borough OD Matrix", "table", "nyc_gold.od_borough_matrix", _T100),

    # ── OPS ──
    ("Peak Hours Heatmap", "table", "nyc_gold.ops_peak_hours_heatmap", _T500),
    ("Trip Distance Distribution", "table", "nyc_gold.ops_trip_distance_distribution", _T100),
    ("Passenger Count Pattern", "table", "nyc_gold.ops_passenger_count_pattern", _T),
    ("Utilization Rate", "echarts_timeseries_line", "nyc_gold.ops_utilization_rate",
     {"metrics": [_m("tip_rate_pct", "AVG"), _m("multi_passenger_pct", "AVG")],
      "granularity_sqla": "pickup_date", "time_range": "No filter"}),

    # ── DQ ──
    ("Data Quality Summary", "table", "nyc_gold.dq_validation_summary", _T100),
    ("Invalid by Reason", "pie", "nyc_gold.dq_invalid_by_reason",
     {"metrics": [_m("count")], "groupby": ["reason"],
      "time_range": "No filter"}),
    ("Row Count Trend", "echarts_timeseries_line", "nyc_gold.dq_row_count_trend",
     {"metrics": [_m("trip_count")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Batch Metadata", "table", "nyc_gold.dq_batch_metadata", {"all_columns": [], "row_limit": 10, "time_range": "No filter"}),
]


def main() -> int:
    # ── Login ──
    token = _req(
        "POST", "/security/login",
        {"username": "admin", "password": "admin", "provider": "db"},
    )["access_token"]

    H = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    def _api(method: str, path: str, payload: dict | None = None) -> dict:
        data_bytes = json.dumps(payload).encode() if payload else None
        req = urllib.request.Request(
            f"{BASE}{path}", data=data_bytes, headers=H, method=method,
        )
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())

    def get(path: str) -> dict:
        return _api("GET", path)

    def post(path: str, payload: dict) -> dict:
        return _api("POST", path, payload)

    def put(path: str, payload: dict) -> dict:
        return _api("PUT", path, payload)

    # ──────────────────────────────────────────────────
    # 1. Register Trino Database
    # ──────────────────────────────────────────────────
    dbs = get("/database/")
    db_name = "NYC Trino"
    db_id = next(
        (r["id"] for r in dbs.get("result", [])
         if r["database_name"] == db_name), None
    )
    if db_id is None:
        resp = post("/database/", {
            "database_name": db_name,
            "sqlalchemy_uri": TRINO_URI,
            "allow_dml": True,
            "expose_in_sqllab": True,
        })
        db_id = resp["id"]
        print(f"[db] created: {db_name} id={db_id}")
    else:
        print(f"[db] exists: {db_name} id={db_id}")

    # ──────────────────────────────────────────────────
    # 2. Register all gold tables as datasets
    # ──────────────────────────────────────────────────
    existing_ds = get("/dataset/?q=(page_size:200)").get("result", [])
    existing_by_key = {
        (r["schema"], r["table_name"]): r["id"] for r in existing_ds
    }

    ds_ids: dict[str, int] = {}
    skipped = 0
    for schema, table in GOLD_TABLES:
        key = (schema, table)
        ds_key_name = f"{schema}.{table}"
        if key in existing_by_key:
            ds_ids[ds_key_name] = existing_by_key[key]
            continue

        try:
            resp = post("/dataset/", {
                "database": db_id,
                "schema": schema,
                "table_name": table,
            })
            ds_id = resp["id"]
            ds_ids[ds_key_name] = ds_id
            print(f"[dataset] {ds_key_name} id={ds_id}")
        except urllib.error.HTTPError as e:
            if e.code == 422:
                skipped += 1
                print(f"[dataset] SKIP {ds_key_name}: {e.code} (table may not exist yet)")
            else:
                raise

    print(f"[dataset] total: {len(ds_ids)} (skipped: {skipped})")

    # ──────────────────────────────────────────────────
    # 3. Dashboard — create first so charts can link to it
    # ──────────────────────────────────────────────────
    dash_slug = "nyc-taxi-gold"
    dash_list = get("/dashboard/")
    dash_id = next(
        (r["id"] for r in dash_list.get("result", [])
         if r.get("slug") == dash_slug), None
    )

    if dash_id is None:
        resp = post("/dashboard/", {
            "dashboard_title": "NYC Taxi Gold Analytics",
            "slug": dash_slug,
            "json_metadata": '{"cross_filters_enabled": false}',
        })
        dash_id = resp["id"]
        print(f"[dashboard] created: id={dash_id}")
    else:
        print(f"[dashboard] exists: id={dash_id}")

    # ──────────────────────────────────────────────────
    # 4. Create charts, linked to dashboard
    # ──────────────────────────────────────────────────
    existing_charts = get("/chart/?q=(page_size:200)").get("result", [])
    existing_by_name: dict[str, dict] = {}
    for c in existing_charts:
        n = c.get("slice_name", "")
        if n not in existing_by_name:
            existing_by_name[n] = c

    chart_ids: dict[str, int] = {}
    for name, viz, ds_key, params in CHART_DEFS:
        ds_id = ds_ids.get(ds_key)
        if ds_id is None:
            print(f"[chart] SKIP {name}: datasource {ds_key} not found")
            continue

        existing = existing_by_name.get(name)
        if existing and existing.get("datasource_id") == ds_id:
            cid = existing["id"]
            chart_ids[name] = cid
            # Update params + dashboard link (idempotent)
            # Always include datasource fields — PUT replaces all fields
            dashboards = existing.get("dashboards", [])
            db_list = [d["id"] if isinstance(d, dict) else d for d in dashboards]
            if dash_id not in db_list:
                db_list.append(dash_id)
            put(f"/chart/{cid}", {
                "datasource_id": ds_id,
                "datasource_type": "table",
                "params": json.dumps(params),
                "dashboards": db_list,
            })
            print(f"[chart] updated {name} id={cid}")
            continue

        # Create new chart with dashboard link
        resp = post("/chart/", {
            "slice_name": name,
            "viz_type": viz,
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
            "dashboards": [dash_id],
        })
        chart_ids[name] = resp["id"]
        print(f"[chart] {name} ({viz}) id={resp['id']}")

    print(
        f"\n{'='*60}\n"
        f"Superset bootstrap complete: "
        f"DB={db_id}, Datasets={len(ds_ids)}, "
        f"Charts={len(chart_ids)}, Dashboard={dash_id}\n"
        f"{'='*60}"
    )
    return 0


def _req(method: str, path: str, data: dict | None = None) -> dict:
    """Unauthenticated request (login only)."""
    headers = {"Content-Type": "application/json"}
    data_bytes = json.dumps(data).encode() if data else None
    r = urllib.request.Request(
        f"{BASE}{path}", data=data_bytes, headers=headers, method=method
    )
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


if __name__ == "__main__":
    sys.exit(main())
