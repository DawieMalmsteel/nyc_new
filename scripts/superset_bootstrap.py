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


def _m(col_name: str, aggregate: str = "SUM", label: str = "") -> dict:
    """Build a metric dict with human-readable label (auto-title-case if not given)."""
    if not label:
        label = col_name.replace("_", " ").title()
    return {"aggregate": aggregate, "column": {"column_name": col_name},
            "expressionType": "SIMPLE", "label": label}


def _d3_format(label: str) -> str:
    """Map metric label to d3 number format."""
    low = label.lower()
    # Percentages / rates (check BEFORE money — "% of Total" should be .1%)
    if any(w in low for w in ("%", "rate", "pct", "share", "ratio")):
        return ".1%"
    # Money: revenue, fare, tip, amount, surcharge, tolls (NOT "total" — too broad, catches counts)
    if any(w in low for w in ("revenue", "fare", "tip", "amount", "surcharge", "tolls")):
        return "$,.0f"
    # Distance
    if "distance" in low:
        return ",.1f"
    # Count / volume (default for integers)
    return ",.0f"


# Sentinels for auto-generated table chart params.
# Row limits tuned to actual table sizes to keep dashboard fast.
_T = "row_limit_50"    # charts needing ~50 rows
_T100 = "row_limit_100"
_T500 = "row_limit_500"

CHART_DEFS = [
    # ═══════════ KPI big numbers ═══════════
    ("All-Time Trip Count", "big_number_total", "kpi_daily_overview",
     {"metric": _m("trips", label="Total Trips"), "time_range": "No filter",
      "subheader": "Jan–Mar 2024"},
     "Total trips across all available data — headline KPI"),
    ("Total Revenue", "big_number_total", "kpi_daily_overview",
     {"metric": _m("revenue", label="Total Revenue"), "time_range": "No filter",
      "subheader": "Jan–Mar 2024"},
     "Gross revenue — fare + tolls + surcharges + tips"),

    # ═══════════ Revenue & Trip Trends ═══════════
    ("Daily Revenue", "echarts_timeseries_bar", "kpi_daily_overview",
     {"metrics": [_m("revenue", label="Revenue")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"},
     "Daily gross revenue — fare + tolls + surcharges + tips"),
    ("Daily Trips", "echarts_timeseries_line", "kpi_daily_overview",
     {"metrics": [_m("trips", label="Trips")], "granularity_sqla": "pickup_date",
      "time_range": "No filter"},
     "Daily trip count — weekday peaks clearly visible"),
    ("Weekly Trip Trends", "echarts_timeseries_bar", "kpi_weekly_trends",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("revenue", label="Revenue")],
      "granularity_sqla": "week_start", "time_range": "No filter"},
     "Weekly trips + revenue — dual-axis for volume vs value comparison"),
    ("Monthly Summary", "dist_bar", "kpi_monthly_summary",
     {"metrics": [_m("total_revenue", label="Revenue")],
      "groupby": ["pickup_month"]},
     "Month-over-month revenue — Jan/Feb/Mar 2024 comparison"),
    ("Borough Trips Over Time", "echarts_timeseries_bar", "fact_trips_borough",
     {"metrics": [_m("trip_count", label="Trips")], "groupby": ["pickup_borough"],
      "granularity_sqla": "pickup_date", "time_range": "No filter"},
     "Daily trips per borough — stacked to compare borough volumes"),
    ("Hourly Trip Pattern", "echarts_timeseries_bar", "fact_trips_hourly",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("revenue", label="Revenue")],
      "groupby": ["pickup_hour"],
      "granularity_sqla": "pickup_date", "time_range": "No filter"},
     "Trip volume + revenue by hour — rush hour peaks at 8AM & 6PM"),

    # ═══════════ Revenue Breakdown (pie) ═══════════
    ("Borough Market Share", "pie", "kpi_borough_comparison",
     {"metrics": [_m("revenue", label="Revenue")], "groupby": ["pickup_borough"],
      "time_range": "No filter"},
     "Revenue share by pickup borough — Manhattan ~85%"),
    ("Payment Types", "pie", "kpi_payment_trends",
     {"metrics": [_m("trip_count", label="Trips")], "groupby": ["payment_desc"],
      "time_range": "No filter"},
     "Trip distribution by payment method — credit card vs cash vs others"),
    ("Vendor Market Share", "pie", "kpi_vendor_performance",
     {"metrics": [_m("trips", label="Trips")], "groupby": ["vendor_id"],
      "time_range": "No filter"},
     "Trip share by vendor — Creative Mobile vs VeriFone"),
    ("Airport Direction", "pie", "route_airport_analysis",
     {"metrics": [_m("trip_count", label="Trips")], "groupby": ["direction"],
      "time_range": "No filter"},
     "Inbound vs outbound airport trips — JFK, LGA, EWR"),

    # ═══════════ Routes & Zones ═══════════
    ("Top Pickup Zones", "dist_bar", "route_top_pickup_zones",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("revenue", label="Revenue")],
      "groupby": ["pickup_zone"]},
     "Top 20 pickup zones by trip volume + revenue"),
    ("Top Dropoff Zones", "dist_bar", "route_top_dropoff_zones",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("revenue", label="Revenue")],
      "groupby": ["dropoff_zone"]},
     "Top 20 dropoff zones — Midtown & Financial District dominate"),
    ("Popular Routes", "dist_bar", "route_popular_routes",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("revenue", label="Revenue")],
      "groupby": ["pickup_zone", "dropoff_zone"]},
     "Most frequent pickup→dropoff zone pairs — top intra-Manhattan routes"),
    ("Airport Trip Stats", "dist_bar", "route_airport_analysis",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("revenue", label="Revenue")],
      "groupby": ["airport"]},
     "Trip volume + revenue per airport — JFK > LGA > EWR"),
    ("Cross-Borough Routes", "dist_bar", "route_cross_borough",
     {"metrics": [_m("trip_count", label="Trips")],
      "groupby": ["pickup_borough", "dropoff_borough"]},
     "Inter-borough trip flows — origin→destination matrix"),
    ("Airport × Zone", "dist_bar", "route_airport_zone_matrix",
     {"metrics": [_m("trips", label="Trips")], "groupby": ["airport_zone"]},
     "Trips from each airport to top destination zones"),
    ("Borough OD Flow", "dist_bar", "od_borough_matrix",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("pct_of_total", label="% of Total", aggregate="AVG")],
      "groupby": ["pickup_borough", "dropoff_borough"]},
     "Borough origin-destination matrix — with % of all trips"),
    ("Zone Performance", "dist_bar", "kpi_zone_performance",
     {"metrics": [_m("pickups", label="Pickups")], "groupby": ["zone"]},
     "Pickup volume by zone — Lower Manhattan & Midtown lead"),
    ("Zone Net Flow", "dist_bar", "kpi_zone_net_flow",
     {"metrics": [_m("net_flow", label="Net Flow")], "groupby": ["zone"]},
     "Net inflow/outflow per zone — positive = arrivals > departures"),
    ("Zone Trip Volume", "dist_bar", "fact_trips_hourly_zone",
     {"metrics": [_m("trip_count", label="Trips")], "groupby": ["pickup_zone"]},
     "Total trips per pickup zone — all-time aggregate ranking"),
    ("Zone Groups (Volume)", "dist_bar", "dim_zone_grouped",
     {"metrics": [_m("pickup_trip_count", label="Pickup Trips")], "groupby": ["zone"]},
     "Zones grouped by volume tier — high/medium/low traffic"),

    # ═══════════ Trip Profile ═══════════
    ("Trip Distance Dist.", "dist_bar", "ops_trip_distance_distribution",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("avg_fare", label="Avg Fare", aggregate="AVG")],
      "groupby": ["distance_bucket"]},
     "Trip distance distribution — majority under 3 miles, avg fare rises with distance"),
    ("Passenger Count Pat.", "dist_bar", "ops_passenger_count_pattern",
     {"metrics": [_m("trip_count", label="Trips")], "groupby": ["passenger_count"]},
     "Passenger count per trip — solo riders dominate (~70%)"),
    ("Peak Hours", "dist_bar", "ops_peak_hours_heatmap",
     {"metrics": [_m("trip_count", label="Trips"),
                  _m("revenue", label="Revenue")],
      "groupby": ["pickup_hour"]},
     "Trips + revenue by hour — morning 8-9AM & evening 6-7PM peaks"),

    # ═══════════ Data Quality ═══════════
    ("Quality Checks", "echarts_timeseries_bar", "dq_validation_summary",
     {"metrics": [_m("total_trips", label="Total Trips")],
      "granularity_sqla": "pickup_date", "time_range": "No filter"},
     "Daily total trips processed — monitors pipeline health"),
    ("Row Count Trend", "echarts_timeseries_line", "dq_row_count_trend",
     {"metrics": [_m("trip_count", label="Trip Count")],
      "granularity_sqla": "pickup_date", "time_range": "No filter"},
     "Daily row counts — detect missing or anomalous data"),

    # ═══════════ Reference Tables ═══════════
    ("Batch Metadata", "table", "dq_batch_metadata", _T,
     "Pipeline batch run metadata — timestamps, file counts, input paths"),
    ("Zone Directory", "table", "dim_zone", _T100,
     "Full NYC taxi zone lookup — 265 zones with borough and service zone"),
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
    chart_meta = {name: (viz, ds_key) for name, viz, ds_key, *_ in CHART_DEFS}
    chart_ids = {}
    for entry in CHART_DEFS:
        name, viz, ds_key, params = entry[:4]
        description = entry[4] if len(entry) > 4 and entry[4] not in (_T, _T100, _T500) else ""
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

        # orderby: skip for pie (backend rejects dimension orderby for pie)
        if viz != "pie":
            ob = params.get("metrics", [{}])[0].get("label")
            if ob:
                params["orderby"] = [[ob, False]]; params["order_desc"] = False

        # Pie charts use singular "metric" control (not "metrics" array)
        if viz == "pie" and "metrics" in params:
            params["metric"] = params.pop("metrics")[0]

        # Cap row_limit to keep charts compact
        if viz == "dist_bar" and "row_limit" not in params:
            params["row_limit"] = 15
        elif viz.startswith("echarts_") and "row_limit" not in params:
            params["row_limit"] = 500
        elif viz == "pie" and "row_limit" not in params:
            params["row_limit"] = 10

        # Inject number formatting based on first metric label
        if viz in ("echarts_timeseries_bar", "echarts_timeseries_line", "echarts_timeseries_step"):
            ml = params.get("metrics", [{}])[0].get("label", "")
            if ml:
                params["y_axis_format"] = _d3_format(ml)
        elif viz == "dist_bar":
            ml = params.get("metrics", [{}])[0].get("label", "")
            if ml:
                params["y_axis_format"] = _d3_format(ml)
        elif viz == "big_number_total":
            ml = params.get("metric", {}).get("label", "")
            if ml:
                params["number_format"] = _d3_format(ml)
        elif viz == "pie":
            ml = params.get("metric", {}).get("label", "")
            if ml:
                params["number_format"] = _d3_format(ml)
                params["label_type"] = "percent"
        elif viz == "table":
            # Format table columns via column_config
            ml = params.get("metrics", [{}])[0].get("label", "") if params.get("metrics") else ""
            if ml:
                params["column_config"] = {
                    ml: {"d3NumberFormat": _d3_format(ml),
                          "columnWidth": 120},
                }

        # Delete old
        old = existing.get(name)
        if old and old.get("datasource_id") == ds_id:
            _api("DELETE", f'/chart/{old["id"]}')
            print(f"[chart] deleted old {name}")

        # Create new chart linked to the dashboard
        chart_payload = {"slice_name": name, "viz_type": viz,
                  "datasource_id": ds_id, "datasource_type": "table",
                  "dashboards": [dash_id],
                  "params": json.dumps(params)}
        if description:
            chart_payload["description"] = description
        cid = post("/chart/", chart_payload)["id"]
        chart_ids[name] = cid
        print(f"[chart] {name} ({viz}) id={cid} — linked to dashboard {dash_id}")

    # 5. Dashboard layout: header + chart-description MARKDOWN widgets.
    # CHART components in position_json crash Superset 4.1.2, but MARKDOWN
    # widgets work fine.  We interleave markdown description cards between
    # chart groups so the auto-layout shows each section's purpose inline.
    html_header = (
        '<div style="font-family:system-ui,sans-serif;padding:16px;max-width:1200px;margin:0 auto">'
        '<h1 style="border-bottom:3px solid #1a73e8;padding-bottom:8px;color:#1a1a2e">'
        'NYC Taxi Gold Analytics</h1>'
        f'<p style="color:#666;font-size:14px">{len(chart_ids)} charts — '
        f'powered by Postgres analytics (2.7M trips, Jan-Mar 2024)</p>'
        '</div>'
    )

    # Group charts by section with a short markdown description card per group
    section_charts = [
        ("📊  Key Performance Indicators", "All-time aggregates: total trips, total revenue across 3 months of data",
         ["All-Time Trip Count", "Total Revenue"]),
        ("📈  Revenue & Trip Trends", "Daily, weekly, monthly, and hourly trip + revenue patterns",
         ["Daily Revenue", "Daily Trips", "Weekly Trip Trends", "Monthly Summary",
          "Borough Trips Over Time", "Hourly Trip Pattern"]),
        ("💰  Revenue Breakdown", "Revenue split by borough, payment type, vendor, airport direction",
         ["Borough Market Share", "Payment Types", "Vendor Market Share", "Airport Direction"]),
        ("🗺️  Routes & Zones", "Top pickup/dropoff zones, popular routes, cross-borough and airport flows",
         ["Top Pickup Zones", "Top Dropoff Zones", "Popular Routes", "Cross-Borough Routes",
          "Airport Trip Stats", "Airport × Zone", "Borough OD Flow",
          "Zone Performance", "Zone Net Flow", "Zone Trip Volume", "Zone Groups (Volume)"]),
        ("👥  Trip Profile", "Passenger demographics, trip distance distribution, peak hour patterns",
         ["Passenger Count Pat.", "Trip Distance Dist.", "Peak Hours"]),
        ("🔍  Data Quality & Reference", "Pipeline health monitoring, row counts, zone directory",
         ["Quality Checks", "Row Count Trend", "Batch Metadata", "Zone Directory"]),
    ]

    ts = int(time.time())
    grid_children = [f"MD-HDR-{ts}"]
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID",
                     "children": grid_children, "parents": ["ROOT_ID"]},
    }
    pos[f"MD-HDR-{ts}"] = {
        "id": f"MD-HDR-{ts}", "type": "MARKDOWN",
        "meta": {"code": html_header, "width": 12, "height": 8},
        "parents": ["ROOT_ID", "GRID_ID"], "children": [],
    }

    for i, (title, desc, _) in enumerate(section_charts):
        card_id = f"MD-CARD-{ts}-{i}"
        card_html = (
            f'<div style="background:#f0f4ff;border-left:4px solid #1a73e8;'
            f'padding:10px 14px;margin:6px 0;border-radius:0 6px 6px 0;'
            f'font-family:system-ui,sans-serif;max-width:1100px">'
            f'<strong style="color:#1a1a2e;font-size:14px">{title}</strong>'
            f'<br><span style="color:#555;font-size:12px">{desc}</span>'
            f'</div>'
        )
        pos[card_id] = {
            "id": card_id, "type": "MARKDOWN",
            "meta": {"code": card_html, "width": 12, "height": 2},
            "parents": ["ROOT_ID", "GRID_ID"], "children": [],
        }
        grid_children.append(card_id)

    put(f"/dashboard/{dash_id}", {"position_json": json.dumps(pos)})
    print(f"[dashboard] MARKDOWN header + {len(chart_ids)} embedded chart views "
          f"(no CHART grid — avoids ChartHolder crash)")

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
