#!/usr/bin/env python3
"""
superset_bootstrap.py — Switch all datasets/charts to Postgres analytics.

Idempotent: skips resources that already exist. Uses REST API.
Registers Postgres analytics DB, creates datasets from 33 gold tables,
and builds a dashboard with 25+ charts.
"""
import json
import os
import sys
import urllib.request
import urllib.error

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088") + "/api/v1"
PG_ANALYTICS_URI = os.environ.get(
    "PG_ANALYTICS_URI",
    "postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics",
)

# ── Gold tables — all 33 materialized into Postgres ──
GOLD_TABLES = [
    # Fact aggregates
    "fact_trips_daily", "fact_trips_hourly", "fact_trips_hourly_zone",
    "fact_trips_borough",
    # Dimensions
    "dim_zone", "dim_zone_grouped", "dim_date", "dim_vendor",
    "dim_payment_type", "dim_rate_code",
    # KPIs
    "kpi_daily_overview", "kpi_weekly_trends", "kpi_monthly_summary",
    "kpi_borough_comparison", "kpi_zone_performance", "kpi_zone_net_flow",
    "kpi_payment_trends", "kpi_vendor_performance",
    # Routes
    "route_top_pickup_zones", "route_top_dropoff_zones", "route_popular_routes",
    "route_airport_analysis", "route_airport_zone_matrix", "route_cross_borough",
    "od_borough_matrix",
    # Operations
    "ops_peak_hours_heatmap", "ops_trip_distance_distribution",
    "ops_passenger_count_pattern", "ops_utilization_rate",
    # Data Quality
    "dq_validation_summary", "dq_invalid_by_reason", "dq_row_count_trend",
    "dq_batch_metadata",
]


def _m(col_name: str, aggregate: str = "SUM") -> dict:
    return {"aggregate": aggregate, "column": {"column_name": col_name},
            "expressionType": "SIMPLE", "label": col_name}


# ── Chart definitions — datasource keys are Postgres table names ──
_T = None
_T100 = "row_limit_100"
_T500 = "row_limit_500"

CHART_DEFS = [
    ("Hourly Trip Pattern", "echarts_timeseries_bar", "fact_trips_hourly",
     {"metrics": [_m("trip_count")], "groupby": ["pickup_hour"],
      "granularity_sqla": "pickup_date", "time_range": "No filter"}),
    ("Hourly Zone Detail", "table", "fact_trips_hourly_zone", _T),
    ("Borough Trip Summary", "table", "fact_trips_borough", _T),
    ("Zone Directory", "table", "dim_zone", _T500),
    ("Zone Groups", "table", "dim_zone_grouped", _T500),
    ("Daily Revenue (KPI)", "echarts_timeseries_bar", "kpi_daily_overview",
     {"metrics": [_m("revenue")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Daily Trips (KPI)", "echarts_timeseries_line", "kpi_daily_overview",
     {"metrics": [_m("trips")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Weekly Trip Trends", "echarts_timeseries_bar", "kpi_weekly_trends",
     {"metrics": [_m("trip_count")], "granularity_sqla": "week_start",
      "time_range": "No filter"}),
    ("Monthly Revenue Growth", "table", "kpi_monthly_summary", _T100),
    ("Borough Market Share", "pie", "kpi_borough_comparison",
     {"metrics": [_m("revenue")], "groupby": ["pickup_borough"],
      "time_range": "No filter"}),
    ("Zone Performance", "table", "kpi_zone_performance", _T),
    ("Zone Net Flow", "table", "kpi_zone_net_flow", _T),
    ("Payment Types", "pie", "kpi_payment_trends",
     {"metrics": [_m("trip_count")], "groupby": ["payment_type"],
      "time_range": "No filter"}),
    ("Vendor Market Share", "pie", "kpi_vendor_performance",
     {"metrics": [_m("trips")], "groupby": ["vendor_id"],
      "time_range": "No filter"}),
    ("Top Pickup Zones", "table", "route_top_pickup_zones", _T100),
    ("Top Dropoff Zones", "table", "route_top_dropoff_zones", _T100),
    ("Popular Routes", "table", "route_popular_routes", _T100),
    ("Airport Trip Analysis", "table", "route_airport_analysis", _T100),
    ("Airport Zone Matrix", "table", "route_airport_zone_matrix", _T),
    ("Cross-Borough Routes", "table", "route_cross_borough", _T100),
    ("Borough OD Matrix", "table", "od_borough_matrix", _T100),
    ("Peak Hours Heatmap", "table", "ops_peak_hours_heatmap", _T500),
    ("Trip Distance Distribution", "table", "ops_trip_distance_distribution", _T100),
    ("Passenger Count Pattern", "table", "ops_passenger_count_pattern", _T),
    ("Data Quality Summary", "table", "dq_validation_summary", _T100),
    ("Row Count Trend", "echarts_timeseries_line", "dq_row_count_trend",
     {"metrics": [_m("trip_count")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Batch Metadata", "table", "dq_batch_metadata", _T),
]


def main() -> int:
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

    # ── 1. Register Postgres analytics DB ──
    dbs = get("/database/")
    pg_db_name = "NYC Analytics (Postgres)"
    pg_db_id = next(
        (r["id"] for r in dbs.get("result", [])
         if r["database_name"] == pg_db_name), None
    )
    if pg_db_id is None:
        resp = post("/database/", {
            "database_name": pg_db_name,
            "sqlalchemy_uri": PG_ANALYTICS_URI,
            "allow_dml": True,
            "expose_in_sqllab": True,
        })
        pg_db_id = resp["id"]
        print(f"[db] created: {pg_db_name} id={pg_db_id}")
    else:
        print(f"[db] exists: {pg_db_name} id={pg_db_id}")

    # ── 2. Register all gold tables as Postgres datasets ──
    existing_ds = get("/dataset/?q=(page_size:200)").get("result", [])
    existing_by_key = {
        (r["schema"], r["table_name"]): r["id"] for r in existing_ds
    }

    ds_ids: dict[str, int] = {}
    skipped = 0
    for table in GOLD_TABLES:
        key = ("public", table)
        ds_key_name = table  # short name for chart refs
        if key in existing_by_key:
            ds_ids[ds_key_name] = existing_by_key[key]
            continue

        try:
            resp = post("/dataset/", {
                "database": pg_db_id,
                "schema": "public",
                "table_name": table,
            })
            ds_ids[ds_key_name] = resp["id"]
            print(f"[dataset] {ds_key_name} id={resp['id']}")
        except urllib.error.HTTPError as e:
            if e.code == 422:
                skipped += 1
                print(f"[dataset] SKIP {ds_key_name}: table may not exist yet")
            else:
                raise

    print(f"[dataset] total: {len(ds_ids)} (skipped: {skipped})")

    # ── 3. Fetch column info per dataset ──
    ds_columns: dict[str, list[str]] = {}
    for ds_key, ds_id in ds_ids.items():
        try:
            info = get(f"/dataset/{ds_id}")
            cols = [c["column_name"] for c in info.get("result", {}).get("columns", [])]
            ds_columns[ds_key] = cols
        except Exception as ex:
            print(f"[warn] {ds_key} columns: {ex}")
            ds_columns[ds_key] = []

    # ── 4. Dashboard ──
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
            "json_metadata": '{"cross_filters_enabled": false, "default_filters": "{}"}',
        })
        dash_id = resp["id"]
        print(f"[dashboard] created: id={dash_id}")
    else:
        print(f"[dashboard] exists: id={dash_id}")

    # ── 5. Create/update charts ──
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

        # Resolve table chart params
        if params is _T or params is _T100 or params is _T500:
            cols = ds_columns.get(ds_key, [])
            groupby_cols = [c for c in cols if not c.startswith("pickup_date")][:4]
            if not groupby_cols:
                groupby_cols = cols[:4]  # fallback: any columns
            rl = 100 if params is _T100 else 500 if params is _T500 else 1000
            params = {"groupby": groupby_cols, "row_limit": rl,
                      "time_range": "No filter"}

        existing = existing_by_name.get(name)

        # Delete+recreate to avoid stale params (orderby, etc.)
        if existing and existing.get("datasource_id") == ds_id:
            cid = existing["id"]
            _api("DELETE", f"/chart/{cid}")
            print(f"[chart] deleted old {name} id={cid} (recreating)")

        # Build params
        if viz == "table" or viz.startswith("echarts"):
            orderby_col = (params.get("groupby", [None])[0]
                           if params.get("groupby") else
                           params.get("metrics", [{}])[0].get("label", None))
            if orderby_col:
                params["orderby"] = [[orderby_col, False]]
                params["order_desc"] = False

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
        f"Superset bootstrap complete:\n"
        f"  DB: {pg_db_name} (id={pg_db_id})\n"
        f"  Datasets: {len(ds_ids)}\n"
        f"  Charts: {len(chart_ids)}\n"
        f"  Dashboard: {dash_id}\n"
        f"{'='*60}"
    )
    return 0


def _req(method: str, path: str, data: dict | None = None) -> dict:
    data_bytes = json.dumps(data).encode() if data else None
    r = urllib.request.Request(
        f"{BASE}{path}", data=data_bytes,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


if __name__ == "__main__":
    sys.exit(main())
