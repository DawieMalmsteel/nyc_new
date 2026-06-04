#!/usr/bin/env python3
"""Compatibility report script (no Spark runtime required)."""

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.dataset as ds


def count_parquet_rows(path: str) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    files = list(p.rglob("*.parquet"))
    if not files:
        return 0
    return ds.dataset(str(p), format="parquet").count_rows()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", default="data/silver/trips")
    parser.add_argument("--quarantine-path", default="data/quarantine/invalid_trips")
    parser.add_argument("--output", default="reports/data_quality_report.md")
    args = parser.parse_args()

    valid_count = count_parquet_rows(args.silver_path)
    invalid_count = count_parquet_rows(args.quarantine_path)
    total = valid_count + invalid_count
    invalid_pct = (invalid_count / total * 100.0) if total > 0 else 0.0

    report = f"""# Data Quality Report

Generated at: {datetime.now(timezone.utc).isoformat()}

- Total records processed: **{total}**
- Valid records: **{valid_count}**
- Invalid records: **{invalid_count}**
- Invalid percentage: **{invalid_pct:.2f}%**
"""

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report)

    print(report)


if __name__ == "__main__":
    main()
