#!/usr/bin/env python3
"""superset_check.py — Verify Superset resources."""
import json, urllib.request, sys

BASE = "http://localhost:8088/api/v1"
data = json.dumps({"username":"admin","password":"admin","provider":"db"}).encode()
req = urllib.request.Request(f"{BASE}/security/login", data=data, headers={"Content-Type":"application/json"})
token = json.loads(urllib.request.urlopen(req).read())["access_token"]
H = {"Authorization": f"Bearer {token}"}

for ep in ["database", "dataset", "chart", "dashboard"]:
    req = urllib.request.Request(f"{BASE}/{ep}/", headers=H)
    resp = json.loads(urllib.request.urlopen(req).read())
    items = resp.get("result", [])
    print(f"{ep}: {len(items)} items")
    for i in items:
        name = i.get("database_name") or i.get("table_name") or i.get("slice_name") or i.get("dashboard_title","?")
        print(f"  [{i['id']}] {name}")
