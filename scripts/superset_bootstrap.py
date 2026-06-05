#!/usr/bin/env python3
"""
superset_bootstrap.py — Register DB, dataset, 4 charts, 1 dashboard via REST API.
Idempotent: skips resources that already exist.
"""
import json
import urllib.request
import sys

BASE = "http://localhost:8088/api/v1"


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
                                   "sqlalchemy_uri": "trino://analytics@trino-coordinator:8080/hive/mart"})
        db_id = resp["id"]
        print(f"  DB created id={db_id}")
    else:
        print(f"  DB exists id={db_id}")

    # 2. Dataset
    ds_list = get("/dataset/")
    ds_id = next((r["id"] for r in ds_list.get("result", [])
                  if r["table_name"] == "fact_trips" and r.get("database", {}).get("id") == db_id), None)
    if ds_id is None:
        resp = post("/dataset/", {"database": db_id, "schema": "mart", "table_name": "fact_trips"})
        ds_id = resp["id"]
        print(f"  dataset created id={ds_id}")
    else:
        print(f"  dataset exists id={ds_id}")

    # 3. Charts
    charts_def = [("trips_per_hour", "bar"), ("top_pickup_zones", "table"),
                  ("borough_revenue", "bar"), ("daily_trips", "line")]
    chart_ids = []
    existing = get("/chart/").get("result", [])
    for name, viz in charts_def:
        found = [r for r in existing if r["slice_name"] == name and r.get("datasource_id") == ds_id]
        if found:
            cid = found[0]["id"]
            print(f"  chart '{name}' exists id={cid}")
        else:
            params = json.dumps({"viz_type": viz, "datasource": f"{ds_id}__table"})
            resp = post("/chart/", {"slice_name": name, "viz_type": viz,
                                    "datasource_id": ds_id, "datasource_type": "table",
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

    print(f"\n  DB:{db_id} Dataset:{ds_id} Charts:{chart_ids} Dashboard:{dash_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
