#!/usr/bin/env bash
# Run the Spark-quality report.
set -euo pipefail
exec python3 /opt/project/jobs/spark_quality_report.py \
  --silver-path "${SILVER_PATH:-data/silver/trips}" \
  --quarantine-path "${QUARANTINE_PATH:-data/quarantine/invalid_trips}" \
  --output "${REPORT_OUTPUT:-reports/data_quality_report.md}"
