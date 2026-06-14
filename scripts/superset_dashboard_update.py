#!/usr/bin/env python3
"""Update Superset dashboard with new datasets and charts.

Idempotent: detects existing resources and updates them.
"""
import json
import sys
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SUPERSET_URL = "http://localhost:39080"
USER = "admin"
PASS = "admin"
TARGET_DB = "NYC Trino"
TARGET_SCHEMA = "mart"
DASHBOARD_TITLE = "NYC Taxi Analytics"
DASHBOARD_SLUG = "nyc-taxi-analytics"


def get_session() -> requests.Session:
    """HTTP session with retries."""
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def wait_for_superset(s: requests.Session, max_wait: int = 180) -> bool:
    """Wait for Superset to respond."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = s.get(f"{SUPERSET_URL}/health", timeout=3)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(2)
    return False


def login(s: requests.Session) -> str:
    """Login to Superset, return access token."""
    r = s.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USER, "password": PASS, "provider": "db"},
        timeout=10,
    )
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token: {r.text}")
    s.headers.update({"Authorization": f"Bearer {token}"})
    return token


def find_database(s: requests.Session) -> int:
    """Get database id for NYC Trino."""
    r = s.get(f"{SUPERSET_URL}/api/v1/database/", timeout=10)
    r.raise_for_status()
    for db in r.json()["result"]:
        if db["database_name"] == TARGET_DB:
            return db["id"]
    raise RuntimeError(f"Database {TARGET_DB!r} not found")


def ensure_dataset(s: requests.Session, db_id: int, table: str) -> int:
    """Create or update dataset for table. Return dataset id."""
    r = s.get(f"{SUPERSET_URL}/api/v1/dataset/", timeout=10)
    r.raise_for_status()
    for ds in r.json()["result"]:
        if ds["table_name"] == table and ds["database"]["id"] == db_id:
            return ds["id"]
    r = s.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        json={"database": db_id, "schema": TARGET_SCHEMA, "table_name": table},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["id"]


def create_chart(s: requests.Session, ds_id: int, name: str, viz: str, query: str) -> int:
    """Create a chart with SQL query. Return chart id."""
    payload = {
        "slice_name": name,
        "viz_type": viz,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": viz,
            "datasource": f"{ds_id}__table",
            "query": query,
        }),
    }
    r = s.post(f"{SUPERSET_URL}/api/v1/chart/", json=payload, timeout=10)
    r.raise_for_status()
    return r.json()["id"]


def find_dashboard(s: requests.Session) -> int | None:
    """Find existing dashboard by slug. Return id or None."""
    r = s.get(f"{SUPERSET_URL}/api/v1/dashboard/", timeout=10)
    r.raise_for_status()
    for d in r.json()["result"]:
        if d.get("slug") == DASHBOARD_SLUG:
            return d["id"]
    return None


def create_dashboard(s: requests.Session, chart_ids: list[int]) -> int:
    """Create new dashboard with charts."""
    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "slug": DASHBOARD_SLUG,
        "owners": [1],
        "position_json": json.dumps({
            "DASHBOARD_CHART_TYPE-1": {
                "type": "CHART",
                "id": "CHART-1",
                "children": [],
                "meta": {
                    "chartId": chart_ids[0],
                    "width": 6, "height": 4,
                },
            } if len(chart_ids) > 0 else None,
        }),
    }
    # Filter out None entries from position_json
    position = {}
    for i, cid in enumerate(chart_ids, 1):
        position[f"DASHBOARD_CHART_TYPE-{i}"] = {
            "type": "CHART",
            "id": f"CHART-{i}",
            "children": [],
            "meta": {
                "chartId": cid,
                "width": 4, "height": 4,
                "row": (i - 1) // 3 * 4,
                "col": (i - 1) % 3 * 4,
            },
        }
    payload["position_json"] = json.dumps(position)
    r = s.post(f"{SUPERSET_URL}/api/v1/dashboard/", json=payload, timeout=10)
    r.raise_for_status()
    return r.json()["id"]


def delete_all_charts(s: requests.Session) -> int:
    """Delete all existing charts. Return count."""
    r = s.get(f"{SUPERSET_URL}/api/v1/chart/", timeout=10)
    r.raise_for_status()
    charts = r.json()["result"]
    for c in charts:
        s.delete(f"{SUPERSET_URL}/api/v1/chart/{c['id']}", timeout=10)
    return len(charts)


def delete_dashboard(s: requests.Session, dash_id: int) -> None:
    """Delete dashboard."""
    s.delete(f"{SUPERSET_URL}/api/v1/dashboard/{dash_id}", timeout=10)


def main() -> int:
    s = get_session()
    print("[superset] waiting for service...", end=" ", flush=True)
    if not wait_for_superset(s):
        print("FAIL (timeout)")
        return 1
    print("OK")

    print("[superset] logging in...", end=" ", flush=True)
    login(s)
    print("OK")

    print("[superset] finding database...", end=" ", flush=True)
    db_id = find_database(s)
    print(f"id={db_id}")

    print("[superset] registering datasets:")
    ds_ids = {}
    for table in ("fact_trips", "mart_revenue_by_day", "mart_revenue_by_zone"):
        ds_id = ensure_dataset(s, db_id, table)
        ds_ids[table] = ds_id
        print(f"  {table}: id={ds_id}")

    print("[superset] cleaning old charts and dashboard...")
    deleted = delete_all_charts(s)
    print(f"  deleted {deleted} old charts")
    old_dash = find_dashboard(s)
    if old_dash:
        delete_dashboard(s, old_dash)
        print(f"  deleted old dashboard id={old_dash}")

    print("[superset] creating new charts:")
    chart_ids = []
    charts = [
        ("Daily Revenue", "table", "mart_revenue_by_day",
         "SELECT pickup_date, trip_count, gross_revenue, avg_fare FROM mart_revenue_by_day ORDER BY pickup_date"),
        ("Top Pickup Zones", "table", "mart_revenue_by_zone",
         "SELECT pickup_borough, pickup_zone, trip_count, gross_revenue FROM mart_revenue_by_zone ORDER BY gross_revenue DESC LIMIT 20"),
        ("Total Trips by Borough", "bar", "fact_trips",
         "SELECT pickup_borough, COUNT(*) AS trip_count FROM fact_trips GROUP BY pickup_borough ORDER BY trip_count DESC"),
        ("Payment Type Distribution", "pie", "fact_trips",
         "SELECT payment_type, COUNT(*) AS trip_count FROM fact_trips GROUP BY payment_type"),
    ]
    for name, viz, table, query in charts:
        cid = create_chart(s, ds_ids[table], name, viz, query)
        chart_ids.append(cid)
        print(f"  '{name}': id={cid} ({viz} on {table})")

    print("[superset] creating dashboard...", end=" ", flush=True)
    dash_id = create_dashboard(s, chart_ids)
    print(f"id={dash_id}")
    print(f"[superset] DONE — dashboard at {SUPERSET_URL}/superset/dashboard/{dash_id}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
