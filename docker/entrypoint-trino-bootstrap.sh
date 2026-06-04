#!/usr/bin/env bash
# Register silver + quarantine tables in the Hive catalog by issuing SQL
# against the Trino coordinator (no docker CLI / trino client required in
# this container — we shell out to a Python helper).
set -euo pipefail

python3 /opt/project/scripts/trino_register.py
