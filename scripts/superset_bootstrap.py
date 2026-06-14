#!/usr/bin/env python3
"""
superset_bootstrap.py — Register Trino DB, gold datasets, charts, and dashboard.

Idempotent: skips resources that already exist. Uses REST API.
Registers all 34 gold tables from hive.nyc_gold as Superset datasets,
plus key mart tables for backward compatibility.
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
    # Fact tables
    ("nyc_gold", "fact_trips_enriched"),
    ("nyc_gold", "fact_trips_daily"),
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


def _req(method: str, path: str, data: dict | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if data is not None:
        data_bytes = json.dumps(data).encode()
    else:
        data_bytes = None
    r = urllib.request.Request(
        f"{BASE}{path}", data=data_bytes, headers=headers, method=method
    )
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


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
    existing_ds = get("/dataset/").get("result", [])
    existing_by_key = {
        (r["schema"], r["table_name"]): r["id"] for r in existing_ds
    }

    ds_ids: dict[str, int] = {}
    for schema, table in GOLD_TABLES:
        key = (schema, table)
        ds_key_name = f"{schema}.{table}"
        if key in existing_by_key:
            ds_ids[ds_key_name] = existing_by_key[key]
            continue

        resp = post("/dataset/", {
            "database": db_id,
            "schema": schema,
            "table_name": table,
        })
        ds_id = resp["id"]
        ds_ids[ds_key_name] = ds_id
        print(f"[dataset] {ds_key_name} id={ds_id}")

    print(f"[dataset] total: {len(ds_ids)}")

    # ──────────────────────────────────────────────────
    # 3. Create charts from key gold datasets
    # ──────────────────────────────────────────────────
    existing_charts = get("/chart/").get("result", [])

    # Chart definitions: (name, viz_type, datasource_key, params)
    CHART_DEFS = [
        # KPI overview
        ("Daily Revenue", "echarts_timeseries_bar", "nyc_gold.kpi_daily_overview",
         {"metrics": ["total_revenue"], "groupby": ["pickup_date"]}),
        ("Daily Trips", "echarts_timeseries_line", "nyc_gold.kpi_daily_overview",
         {"metrics": ["trip_count"], "groupby": ["pickup_date"]}),
        ("Revenue by Borough", "pie", "nyc_gold.kpi_borough_comparison",
         {"metrics": ["total_revenue"], "groupby": ["borough"]}),
        ("Payment Trends", "echarts_timeseries_bar", "nyc_gold.kpi_payment_trends",
         {"metrics": ["trip_count"], "groupby": ["payment_type"]}),
        ("Top Pickup Zones", "table", "nyc_gold.route_top_pickup_zones",
         {}),
        ("Airport Trip Analysis", "table", "nyc_gold.route_airport_analysis",
         {}),
        ("Zone Performance", "table", "nyc_gold.kpi_zone_performance",
         {}),
        ("Borough OD Matrix", "table", "nyc_gold.od_borough_matrix",
         {}),
        ("Hourly Heatmap", "echarts_timeseries_bar", "nyc_gold.fact_trips_hourly",
         {"metrics": ["trip_count"], "groupby": ["pickup_hour"]}),
        ("Data Quality Summary", "table", "nyc_gold.dq_validation_summary",
         {}),
        ("Tip Rate by Zone", "table", "nyc_gold.kpi_zone_net_flow",
         {}),
        ("Weekly Trends", "echarts_timeseries_bar", "nyc_gold.kpi_weekly_trends",
         {"metrics": ["trip_count"], "groupby": ["week_start"]}),
    ]

    chart_ids: dict[str, int] = {}
    for name, viz, ds_key, params in CHART_DEFS:
        ds_id = ds_ids.get(ds_key)
        if ds_id is None:
            print(f"[chart] SKIP {name}: datasource {ds_key} not found")
            continue

        found = [
            r for r in existing_charts
            if r["slice_name"] == name
            and r.get("datasource_id") == ds_id
        ]
        if found:
            chart_ids[name] = found[0]["id"]
            continue

        payload = {
            "slice_name": name,
            "viz_type": viz,
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        resp = post("/chart/", payload)
        chart_ids[name] = resp["id"]
        print(f"[chart] {name} ({viz}) id={resp['id']}")

    # ──────────────────────────────────────────────────
    # 4. Dashboard — NYC Taxi Gold Analytics
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
        })
        dash_id = resp["id"]
        print(f"[dashboard] created: id={dash_id}")
    else:
        print(f"[dashboard] exists: id={dash_id}")

    # Add charts to dashboard
    existing_dash_charts = get(f"/dashboard/{dash_id}/charts").get("result", [])
    existing_chart_ids = {c["id"] for c in existing_dash_charts}
    added = 0
    for name, cid in chart_ids.items():
        if cid not in existing_chart_ids:
            post(f"/dashboard/{dash_id}/charts", {
                "chart_id": cid,
            })
            added += 1
    if added:
        print(f"[dashboard] added {added} charts")

    print(
        f"\n{'='*60}\n"
        f"Superset bootstrap complete: "
        f"DB={db_id}, Datasets={len(ds_ids)}, "
        f"Charts={len(chart_ids)}, Dashboard={dash_id}\n"
        f"{'='*60}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
