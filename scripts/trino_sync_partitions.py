#!/usr/bin/env python3
"""Sync hive.nyc partition metadata for both tables (FULL mode)."""
import os
import sys
import time

from trino.dbapi import connect
from trino.exceptions import TrinoUserError


def wait_for_trino(host: str, port: int, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = connect(host=host, port=port, user="bootstrap")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2)
    raise SystemExit(f"trino not ready: {last_err}")


def main() -> int:
    host = os.environ.get("TRINO_HOST", "trino-coordinator")
    port = int(os.environ.get("TRINO_PORT", "8080"))
    wait_for_trino(host, port)

    conn = connect(host=host, port=port, user="bootstrap")
    cur = conn.cursor()
    for table in ("trips", "invalid_trips"):
        try:
            cur.execute(
                f"CALL hive.system.sync_partition_metadata("
                f"schema_name => 'nyc', table_name => '{table}', mode => 'FULL')"
            )
            cur.fetchall()
            print(f"[sync] {table} OK")
        except TrinoUserError as e:
            print(f"[sync] {table} skipped: {e}", file=sys.stderr)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
