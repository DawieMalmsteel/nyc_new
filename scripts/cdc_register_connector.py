#!/usr/bin/env python3
"""scripts/cdc_register_connector.py — Register Debezium Postgres connector.

Connects to Debezium Kafka Connect REST API and registers a Postgres connector
to capture CDC events from the nyc_taxi.trips table.

Usage:
    python3 scripts/cdc_register_connector.py [--debezium-url http://debezium:8083]
"""
import argparse
import json
import sys
import urllib.request
import urllib.error
import time

CONNECTOR_NAME = "nyc-postgres-connector"
CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "nyc_postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "nyc_taxi",
        "topic.prefix": "nyc_cdc",
        "schema.include.list": "public",
        "table.include.list": "public.trips",
        "plugin.name": "pgoutput",
        "publication.autocreate.mode": "filtered",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "tombstones.on.delete": "false",
    },
}


def _req(url, method="GET", data=None):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    body = json.dumps(data).encode() if data else None
    r = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(r, timeout=10)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"  HTTP {e.code}: {body}", file=sys.stderr)
        raise
    except urllib.error.URLError as e:
        print(f"  Connection failed: {e.reason}", file=sys.stderr)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debezium-url", default="http://debezium:8083")
    args = parser.parse_args()
    base = args.debezium_url.rstrip("/")

    # Wait for Debezium to be ready
    print(f"[cdc] waiting for Debezium at {base} ...")
    for attempt in range(30):
        try:
            info = _req(f"{base}/")
            print(f"[cdc] Debezium ready: {info.get('version', '?')}")
            break
        except Exception:
            time.sleep(2)
    else:
        print("[cdc] Debezium not reachable after 60s")
        return 1

    # Delete existing connector if present
    try:
        existing = _req(f"{base}/connectors/{CONNECTOR_NAME}")
        if existing:
            print(f"[cdc] connector '{CONNECTOR_NAME}' exists, deleting ...")
            _req(f"{base}/connectors/{CONNECTOR_NAME}", method="DELETE")
            time.sleep(1)
    except Exception:
        pass

    # Register new connector
    print(f"[cdc] registering connector '{CONNECTOR_NAME}' ...")
    result = _req(f"{base}/connectors/", method="POST", data=CONNECTOR_CONFIG)
    print(f"[cdc] connector registered: {result['name']} (status: {result.get('state', '?')})")

    # Verify
    status = _req(f"{base}/connectors/{CONNECTOR_NAME}/status")
    print(f"[cdc] connector status: {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
