#!/usr/bin/env python3
"""
superset_bootstrap.py — Register DB, dataset, 4 charts, 1 dashboard via REST API.
Idempotent: skips resources that already exist.
"""
import json
import os
import sys
import urllib.request

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088") + "/api/v1"
TRINO_URI = os.environ.get("TRINO_URI", "trino://analytics@trino-coordinator:8080/hive/mart")


def _req(method, path, data=None):
    headers = {"Content-Type": "application/json"}
    if data and isinstance(data, dict):
        data = json.dumps(data).encode()
    r = urllib.request.Request(f"{BASE}{path}", data=data, headers=headers, method=method)
    return json.loads(urllib.request.urlopen(r).read())


def _login():
    resp = _req("POST", "/security/login",
                {"username": "admin", "password": "admin", "provider": "db"})
    return resp["access_token"]


def main():
    token = _login()
    H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    auth_req = lambda m, p, d=None: json.loads(urllib.request.urlopen(
        urllib.request.Request(f"{BASE}{p}", data=json.dumps(d).encode() if d else None,
                               headers=H, method=m)).read()) if d else \
        json.loads(urllib.request.urlopen(urllib.request.Request(f"{BASE}{p}", headers=H)).read())

    def get(path): return auth_req("GET", path)
    def post(path, payload): return auth_req("POST", path, payload)

    # 1. Database
    dbs = get("/database/")
    db_id = next((r["id"] for r in dbs.get("result", []) if r["database_name"] == "NYC Trino"), None)
    if db_id is None:
        resp = post("/database/", {"database_name": "NYC Trino",
                                   "sqlalchemy_uri": TRINO_URI})
        db_id = resp["id"]
        print(f"  DB created id={db_id}")
    else:
        print(f"  DB exists id={db_id}")

    # 2. Datasets (marts)
    ds_list = get("/dataset/")
    mart_tables = ["fact_trips", "dim_zone", "mart_hourly_summary", "mart_payment_type_summary",
                   "mart_revenue_by_day", "mart_revenue_by_zone", "gold_fact_trips"]
    ds_ids = {}
    for tbl in mart_tables:
        ds_id = next((r["id"] for r in ds_list.get("result", [])
                      if r["table_name"] == tbl and r.get("database", {}).get("id") == db_id), None)
        if ds_id is None:
            resp = post("/dataset/", {"database": db_id, "schema": "mart", "table_name": tbl})
            ds_id = resp["id"]
            print(f"  dataset '{tbl}' created id={ds_id}")
        else:
            print(f"  dataset '{tbl}' exists id={ds_id}")
        ds_ids[tbl] = ds_id

    # 3. Charts
    ft_id = ds_ids["fact_trips"]
    charts_def = [("trips_per_hour", "bar"), ("top_pickup_zones", "table"),
                  ("borough_revenue", "bar"), ("daily_trips", "line")]
    chart_ids = []
    existing = get("/chart/").get("result", [])
    for name, viz in charts_def:
        found = [r for r in existing if r["slice_name"] == name and r.get("datasource_id") == ft_id]
        if found:
            cid = found[0]["id"]
            print(f"  chart '{name}' exists id={cid}")
        else:
            params = json.dumps({"viz_type": viz, "datasource": f"{ft_id}__table"})
            resp = post("/chart/", {"slice_name": name, "viz_type": viz,
                                    "datasource_id": ft_id, "datasource_type": "table",
                                    "params": params})
            cid = resp["id"]
            print(f"  chart '{name}' created id={cid}")
        chart_ids.append(cid)

    # 4. Dashboard
    dash_list = get("/dashboard/")
    dash_id = next((r["id"] for r in dash_list.get("result", []) if r.get("slug") == "nyc-taxi"), None)
    if dash_id is None:
        resp = post("/dashboard/", {"dashboard_title": "NYC Taxi Overview",
                                    "slug": "nyc-taxi", "published": True})
        dash_id = resp["id"]
        print(f"  dashboard created id={dash_id}")
    else:
        print(f"  dashboard exists id={dash_id}")

    print(f"\n  DB:{db_id} Datasets:{list(ds_ids.keys())} Charts:{chart_ids} Dashboard:{dash_id}")


if __name__ == "__main__":
    sys.exit(main())
