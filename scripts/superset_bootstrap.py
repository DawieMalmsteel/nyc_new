#!/usr/bin/env python3
"""superset_bootstrap.py — All Superset datasets/charts/dashboard on Postgres.

Idempotent; skips existing resources. Builds clean position_json to
avoid stale dashboard form_data cache.
"""
import json, os, sys, time, urllib.request, urllib.error

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088") + "/api/v1"
PG_ANALYTICS_URI = os.environ.get(
    "PG_ANALYTICS_URI",
    "postgresql://analytics:analytics@svc-postgres-analytics:5432/nyc_analytics",
)

GOLD_TABLES = [
    "fact_trips_daily", "fact_trips_hourly", "fact_trips_hourly_zone",
    "fact_trips_borough",
    "dim_zone", "dim_zone_grouped", "dim_date", "dim_vendor",
    "dim_payment_type", "dim_rate_code",
    "kpi_daily_overview", "kpi_weekly_trends", "kpi_monthly_summary",
    "kpi_borough_comparison", "kpi_zone_performance", "kpi_zone_net_flow",
    "kpi_payment_trends", "kpi_vendor_performance",
    "route_top_pickup_zones", "route_top_dropoff_zones", "route_popular_routes",
    "route_airport_analysis", "route_airport_zone_matrix", "route_cross_borough",
    "od_borough_matrix",
    "ops_peak_hours_heatmap", "ops_trip_distance_distribution",
    "ops_passenger_count_pattern", "ops_utilization_rate",
    "dq_validation_summary", "dq_invalid_by_reason", "dq_row_count_trend",
    "dq_batch_metadata",
]


def _m(col_name: str, aggregate: str = "SUM") -> dict:
    return {"aggregate": aggregate, "column": {"column_name": col_name},
            "expressionType": "SIMPLE", "label": col_name}


# Sentinels for auto-generated table chart params.
# Row limits tuned to actual table sizes to keep dashboard fast.
_T = "row_limit_50"    # charts needing ~50 rows
_T100 = "row_limit_100"
_T500 = "row_limit_500"

CHART_DEFS = [
    # echarts
    ("Hourly Trip Pattern", "echarts_timeseries_bar", "fact_trips_hourly",
     {"metrics": [_m("trip_count")], "groupby": ["pickup_hour"],
      "granularity_sqla": "pickup_date", "time_range": "No filter"}),
    ("Daily Revenue (KPI)", "echarts_timeseries_bar", "kpi_daily_overview",
     {"metrics": [_m("revenue")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Daily Trips (KPI)", "echarts_timeseries_line", "kpi_daily_overview",
     {"metrics": [_m("trips")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    ("Weekly Trip Trends", "echarts_timeseries_bar", "kpi_weekly_trends",
     {"metrics": [_m("trip_count")], "granularity_sqla": "week_start",
      "time_range": "No filter"}),
    ("Row Count Trend", "echarts_timeseries_line", "dq_row_count_trend",
     {"metrics": [_m("trip_count")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"}),
    # pie
    ("Borough Market Share", "pie", "kpi_borough_comparison",
     {"metrics": [_m("revenue")], "groupby": ["pickup_borough"],
      "time_range": "No filter"}),
    ("Payment Types", "pie", "kpi_payment_trends",
     {"metrics": [_m("trip_count")], "groupby": ["payment_type"],
      "time_range": "No filter"}),
    ("Vendor Market Share", "pie", "kpi_vendor_performance",
     {"metrics": [_m("trips")], "groupby": ["vendor_id"],
      "time_range": "No filter"}),
    # tables — small
    ("Monthly Revenue Growth", "table", "kpi_monthly_summary", _T100),
    ("Top Pickup Zones", "table", "route_top_pickup_zones", _T100),
    ("Top Dropoff Zones", "table", "route_top_dropoff_zones", _T100),
    ("Popular Routes", "table", "route_popular_routes", _T100),
    ("Airport Trip Analysis", "table", "route_airport_analysis", _T100),
    ("Cross-Borough Routes", "table", "route_cross_borough", _T100),
    ("Borough OD Matrix", "table", "od_borough_matrix", _T100),
    ("Trip Distance Distribution", "table", "ops_trip_distance_distribution", _T100),
    ("Data Quality Summary", "table", "dq_validation_summary", _T100),
    ("Batch Metadata", "table", "dq_batch_metadata", _T),
    ("Borough Trip Summary", "table", "fact_trips_borough", _T100),
    # tables — heavy (reduced row_limit to keep dashboard fast)
    ("Hourly Zone Detail", "table", "fact_trips_hourly_zone", _T),
    ("Zone Performance", "table", "kpi_zone_performance", _T),
    ("Zone Net Flow", "table", "kpi_zone_net_flow", _T),
    ("Airport Zone Matrix", "table", "route_airport_zone_matrix", _T),
    ("Passenger Count Pattern", "table", "ops_passenger_count_pattern", _T),
    # tables — medium
    ("Zone Directory", "table", "dim_zone", _T100),
    ("Zone Groups", "table", "dim_zone_grouped", _T100),
    ("Peak Hours Heatmap", "table", "ops_peak_hours_heatmap", _T100),
]


def main() -> int:
    token = _req("POST", "/security/login",
                 {"username": "admin", "password": "admin", "provider": "db"})["access_token"]
    H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _api(method, path, payload=None):
        body = json.dumps(payload).encode() if payload else None
        req = urllib.request.Request(f"{BASE}{path}", data=body, headers=H, method=method)
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    get = lambda p: _api("GET", p)
    post = lambda p, d: _api("POST", p, d)
    put = lambda p, d: _api("PUT", p, d)

    # 1. Postgres DB
    dbs = get("/database/")
    pg_id = next((r["id"] for r in dbs["result"] if "Postgres" in r["database_name"]), None)
    if not pg_id:
        pg_id = post("/database/", {"database_name": "NYC Analytics (Postgres)",
                     "sqlalchemy_uri": PG_ANALYTICS_URI, "allow_dml": True,
                     "expose_in_sqllab": True})["id"]
        print(f"[db] created: id={pg_id}")
    else:
        print(f"[db] exists: id={pg_id}")

    # 2. Datasets
    existing_ds = get("/dataset/?q=(page_size:200)")["result"]
    by_key = {(r["schema"], r["table_name"]): r["id"] for r in existing_ds}
    ds_ids, skipped = {}, 0
    for tbl in GOLD_TABLES:
        k = ("public", tbl)
        if k in by_key:
            ds_ids[tbl] = by_key[k]; continue
        try:
            ds_ids[tbl] = post("/dataset/",
                {"database": pg_id, "schema": "public", "table_name": tbl})["id"]
            print(f"[dataset] {tbl} id={ds_ids[tbl]}")
        except urllib.error.HTTPError as e:
            if e.code == 422: skipped += 1; print(f"[dataset] SKIP {tbl}")
            else: raise
    print(f"[dataset] total: {len(ds_ids)} (skipped {skipped})")

    # Column info for table chart auto-params
    ds_cols = {}
    for k, did in ds_ids.items():
        try:
            info = get(f"/dataset/{did}")
            ds_cols[k] = [c["column_name"] for c in info["result"]["columns"]]
        except Exception:
            ds_cols[k] = []

    # 3. Dashboard
    dash_slug = "nyc-taxi-gold"
    dash_list = get("/dashboard/")
    dash_id = next((r["id"] for r in dash_list["result"] if r["slug"] == dash_slug), None)
    if not dash_id:
        dash_id = post("/dashboard/", {"dashboard_title": "NYC Taxi Gold Analytics",
                        "slug": dash_slug})["id"]
        print(f"[dashboard] created id={dash_id}")
    else:
        print(f"[dashboard] exists id={dash_id}")

    # 4. Charts — delete old, create new, rebuild position_json
    existing = {c["slice_name"]: c for c in get("/chart/?q=(page_size:200)")["result"]}
    chart_meta = {name: (viz, ds_key) for name, viz, ds_key, _ in CHART_DEFS}
    chart_ids = {}
    for name, viz, ds_key, params in CHART_DEFS:
        ds_id = ds_ids.get(ds_key)
        if not ds_id:
            print(f"[chart] SKIP {name}: ds not found"); continue

        # Auto-generate table params
        if params is _T or params is _T100 or params is _T500:
            cols = ds_cols.get(ds_key, [])
            # Groupby only on dimension columns (skip date/timestamp and measures)
            MEASURE_COLS = {"trips", "trip_count", "revenue", "total_revenue",
                            "avg_fare", "avg_tip", "avg_tip_pct", "avg_distance",
                            "count", "pickup_count", "dropoff_count",
                            "net_flow", "net_flow_ratio", "imbalance_score",
                            "passenger_count", "tip_amount", "fare_amount",
                            "total_amount", "trip_distance", "utilization_rate",
                            "unique_vendors", "market_share_pct"}
            groupby = [c for c in cols
                       if not c.startswith("pickup_")
                       and not c.startswith("dropoff_")
                       and not c.endswith("_at")
                       and c not in MEASURE_COLS][:4] or cols[:4]
            rl = 100 if params is _T100 else 500 if params is _T500 else 50
            # Add COUNT metric so Superset uses aggregate mode (not raw record scan)
            params = {
                "groupby": groupby,
                "row_limit": rl,
                "time_range": "No filter",
                "metrics": [{"aggregate": "COUNT",
                             "column": {"column_name": groupby[0] if groupby else "*"},
                             "expressionType": "SIMPLE",
                             "label": "count"}],
            }

        # orderby for all chart types (prevents dashboard null orderby error)
        ob = (params.get("groupby", [None])[0]
              if params.get("groupby") else
              params.get("metrics", [{}])[0].get("label"))
        if ob:
            params["orderby"] = [[ob, False]]; params["order_desc"] = False
        else:
            params["order_desc"] = False

        # Delete old
        old = existing.get(name)
        if old and old.get("datasource_id") == ds_id:
            _api("DELETE", f'/chart/{old["id"]}')
            print(f"[chart] deleted old {name}")

        # Create new (no dashboard attachment — layout rebuilt separately)
        cid = post("/chart/", {"slice_name": name, "viz_type": viz,
                  "datasource_id": ds_id, "datasource_type": "table",
                  "params": json.dumps(params)})["id"]
        chart_ids[name] = cid
        print(f"[chart] {name} ({viz}) id={cid}")

    # 5. Build position_json with proper react-grid-layout fields
    # Superset 4.1.2 frontend uses react-grid-layout and expects each
    # component to have x/y/w/h grid fields plus id/type/meta/children/parents.
    # Without these, the layout engine throws
    # "findFirstParentContainer.js: t is undefined".
    pos = {"DASHBOARD_VERSION_KEY": "v2"}
    COLS = 3  # 3 charts per row (each 4 cols wide on 12-col grid)
    W = 12 // COLS
    ROW_H = 6
    pos["ROOT_ID"] = {
        "id": "ROOT_ID", "type": "ROOT",
        "children": ["GRID_ID"],
    }
    pos["GRID_ID"] = {
        "id": "GRID_ID", "type": "GRID",
        "children": [f"CHART-{i}" for i in range(len(chart_ids))],
        "parents": ["ROOT_ID"],
    }
    for i, (name, cid) in enumerate(chart_ids.items()):
        col = i % COLS
        row = i // COLS
        viz, ds_key = chart_meta[name]
        pos[f"CHART-{i}"] = {
            "id": f"CHART-{i}",
            "type": "CHART",
            "x": col * W, "y": row * ROW_H, "w": W, "h": ROW_H,
            "meta": {
                "chartId": cid, "width": W, "height": ROW_H,
                "sliceName": name,
                "text": "",
            },
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [],
        }
    put(f"/dashboard/{dash_id}", {"position_json": json.dumps(pos)})
    print(f"[dashboard] layout rebuilt: {len(chart_ids)} chart slots with x/y/w/h")

    print(f"\n{'='*60}\nDone: DB={pg_id}, Datasets={len(ds_ids)}, "
          f"Charts={len(chart_ids)}, Dashboard={dash_id}\n{'='*60}")
    return 0


def _req(method, path, data=None):
    body = json.dumps(data).encode() if data else None
    r = urllib.request.Request(f"{BASE}{path}", data=body,
        headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


if __name__ == "__main__":
    sys.exit(main())
