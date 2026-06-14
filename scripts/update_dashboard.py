#!/usr/bin/env python3
"""Superset dashboard update script."""
import json, sys, time, urllib.request, urllib.error

API = "http://localhost:39080/api/v1"
USER, PASS = "admin", "admin"

def login():
    data = json.dumps({"username": USER, "password": PASS, "provider": "db"}).encode()
    r = urllib.request.urlopen(f"{API}/security/login", data=data, timeout=10)
    return json.loads(r.read())["access_token"]

TOKEN = login()
HEADERS = {"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"}

def api(method, path, data=None):
    url = f"{API}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=HEADERS, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()[:200]
        print(f"  {method} {path}: {e.code} {err}")
        return None

# Find Trino DB
print("=== DB ===")
dbs = api("GET", "/database/")
db_id = None
for db in (dbs or {}).get("result", []):
    if db["database_name"] == "NYC Trino":
        db_id = db["id"]
print(f"DB ID: {db_id}")

# Register datasets
print("=== Datasets ===")
ds_map = {}
for table in ["fact_trips", "mart_revenue_by_day", "mart_revenue_by_zone"]:
    r = api("POST", "/dataset/", {"database": db_id, "schema": "mart", "table_name": table})
    if r:
        ds_map[table] = r["id"]
        print(f"  {table}: id={r['id']}")
    else:
        # already exists
        ds_list = api("GET", "/dataset/") or {}
        for ds in ds_list.get("result", []):
            if ds["table_name"] == table and ds["database"]["id"] == db_id:
                ds_map[table] = ds["id"]
                print(f"  {table}: existed id={ds['id']}")
                break

ds_fact = ds_map.get("fact_trips")
if not ds_fact:
    print("ERROR: fact_trips not found")
    sys.exit(1)

# Delete old charts
print("=== Cleaning ===")
charts = api("GET", "/chart/") or {}
for c in charts.get("result", []):
    api("DELETE", f"/chart/{c['id']}")
    print(f"  Deleted chart: {c['slice_name']}")

# Create 5 charts
print("=== Charts ===")
charts_data = [
    ("Trips per Hour", "dist_bar", {
        "granularity_sqla": "pickup_date", "time_range": "No filter",
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "trip_id"}, "aggregate": "COUNT", "label": "Trip Count"}],
        "groupby": ["pickup_hour"], "order_desc": True,
    }),
    ("Daily Revenue", "line", {
        "granularity_sqla": "pickup_date", "time_range": "No filter",
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_amount"}, "aggregate": "SUM", "label": "Revenue"}],
        "order_desc": True,
    }),
    ("Top Pickup Zones", "table", {
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "trip_id"}, "aggregate": "COUNT", "label": "Trip Count"}],
        "groupby": ["pickup_zone"], "order_desc": True, "row_limit": 15,
    }),
    ("Revenue by Borough", "bar", {
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_amount"}, "aggregate": "SUM", "label": "Revenue"}],
        "groupby": ["pickup_borough"], "order_desc": True,
    }),
    ("Payment Type", "pie", {
        "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "trip_id"}, "aggregate": "COUNT", "label": "Trip Count"}],
        "groupby": ["payment_type"], "order_desc": True,
    }),
]

chart_ids = []
for name, viz, params in charts_data:
    payload = {
        "slice_name": name, "viz_type": viz,
        "datasource_id": ds_fact, "datasource_type": "table",
        "params": json.dumps({"viz_type": viz, "datasource": f"{ds_fact}__table", **params}),
    }
    r = api("POST", "/chart/", payload)
    if r:
        cid = r.get("id")
        chart_ids.append(cid)
        print(f"  '{name}': id={cid}")
    else:
        print(f"  '{name}': FAILED")

# Dashboard
print("=== Dashboard ===")
dash_list = api("GET", "/dashboard/") or {}
for d in dash_list.get("result", []):
    if d.get("slug") in ("nyc-taxi", "nyc-taxi-analytics"):
        api("DELETE", f"/dashboard/{d['id']}")
        print(f"  Deleted old dashboard: {d['dashboard_title']}")

position = {}
for i, cid in enumerate(chart_ids):
    position[f"CHART-{i+1}"] = {
        "type": "CHART", "id": f"CHART-{i+1}", "children": [],
        "meta": {"chartId": cid, "width": 4, "height": 4, "row": (i//4)*4, "col": (i%4)*4},
    }

payload = {
    "dashboard_title": "NYC Taxi Overview", "slug": "nyc-taxi", "published": True,
    "position_json": json.dumps(position),
    "json_metadata": json.dumps({"color_scheme": "supersetColors"}),
}
r = api("POST", "/dashboard/", payload)
if r:
    did = r.get("id") or r.get("result", {}).get("id")
    print(f"Dashboard id={did}")
    print(f"→ http://localhost:39080/superset/dashboard/{did}/")
else:
    print("Dashboard FAILED")
