#!/usr/bin/env bash
# Local E2E test — fully dockerized.
# All non-infra steps run via `docker compose run --rm <service>`.
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
HOST_BOOTSTRAP="${HOST_BOOTSTRAP:-localhost:29092}"
INPUT_ARGS="${INPUT_ARGS:-data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet}"
MAX_EVENTS="${MAX_EVENTS:-5000}"
INVALID_RATE="${INVALID_RATE:-0.02}"
EVENTS_PER_SECOND="${EVENTS_PER_SECOND:-0}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
FLUSH_EVERY="${FLUSH_EVERY:-5000}"
TOPIC_RUN="taxi.trip.events.$(date +%s)"

mkdir -p reports

echo "[1/8] Start infra (zookeeper, kafka, kafka-ui, minio, spark-master, spark-worker)"
docker compose up -d zookeeper kafka kafka-ui minio spark-master spark-worker

echo "[2/8] Wait Kafka ready (host port ${HOST_BOOTSTRAP})"
python3 - <<'PY'
import time, sys
from kafka import KafkaAdminClient
for i in range(30):
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:29092')
        admin.close()
        print('[ok] Kafka is ready')
        sys.exit(0)
    except Exception:
        if i == 29:
            raise SystemExit('Kafka not ready after 60s')
        time.sleep(2)
PY

echo "[3/8] Create Kafka topics (in-container)"
docker compose run --rm topic-init
TOPIC_RUN="$TOPIC_RUN" docker compose run --rm --no-deps topic-run
echo "[4/8] Download source data"
bash scripts/download_data.sh

for f in ${INPUT_ARGS}; do
  if [[ ! -f "$f" ]]; then
    echo "[error] input parquet missing: $f"
    exit 1
  fi
done

echo "[5/8] Clean old outputs (in-container)"
docker compose exec -T spark-master bash -lc 'rm -rf /opt/project/data/silver/trips/* /opt/project/data/quarantine/invalid_trips/* /opt/project/data/checkpoints/spark_stream_taxi_events* || true'

echo "[6/8] Publish events to Kafka (in-container)"
docker compose run --rm \
  -e TOPIC="$TOPIC_RUN" \
  -e INPUT_ARGS="$INPUT_ARGS" \
  -e MAX_EVENTS="$MAX_EVENTS" \
  -e INVALID_RATE="$INVALID_RATE" \
  -e EVENTS_PER_SECOND="$EVENTS_PER_SECOND" \
  -e BATCH_SIZE="$BATCH_SIZE" \
  -e FLUSH_EVERY="$FLUSH_EVERY" \
  generator

echo "[7/8] Run stream processor (Kafka -> silver/quarantine, in-container)"
TOPIC="$TOPIC_RUN" bash scripts/start_streaming_job_docker.sh
PROCESSOR_LABEL="Spark(docker)"

echo "[8/8] Build quality report + assertions (in-container)"
docker compose run --rm quality-report

python3 - <<PY
import os
from pathlib import Path
import pyarrow.dataset as ds

processor_label = "${PROCESSOR_LABEL}"

silver_path = Path('data/silver/trips')
invalid_path = Path('data/quarantine/invalid_trips')

def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    files = list(path.rglob('*.parquet'))
    if not files:
        return 0
    return ds.dataset(str(path), format='parquet').count_rows()

valid = count_rows(silver_path)
invalid = count_rows(invalid_path)
total = valid + invalid
invalid_pct = (invalid / total * 100.0) if total else 0.0

status = 'PASS' if (valid > 0 and invalid > 0 and total > 0) else 'FAIL'

report = f'''# Local E2E Test Report

Status: **{status}**

- Valid records: **{valid}**
- Invalid records: **{invalid}**
- Total records: **{total}**
- Invalid percentage: **{invalid_pct:.2f}%**

## Checks

- Kafka running: PASS
- Topic creation: PASS
- Generator publish: PASS
- Processor ({processor_label}) + write silver: {'PASS' if valid > 0 else 'FAIL'}
- Processor ({processor_label}) + write quarantine: {'PASS' if invalid > 0 else 'FAIL'}
'''

os.makedirs('reports', exist_ok=True)
Path('reports/local_test_report.md').write_text(report, encoding='utf-8')
print(report)

if status != 'PASS':
    raise SystemExit(1)
PY

echo "[done] Local e2e test finished successfully"
