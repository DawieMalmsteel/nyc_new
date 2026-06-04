# NYC Taxi Kafka-first Local Pipeline (MVP)

## What is implemented

- Kafka + Kafka UI + MinIO via Docker Compose
- NYC TLC data downloader (3 months + zone lookup)
- Python Kafka generator from Parquet (`generator/taxi_event_generator.py`)
- Spark Structured Streaming consumer from Kafka (`jobs/spark_stream_taxi_events.py`) via Docker Spark
- Local Python stream processor fallback (`jobs/kafka_stream_processor.py`)
- Data quality validation + split valid/invalid
- Output to local lake paths:
  - `data/silver/trips`
  - `data/quarantine/invalid_trips`
- Local E2E test script: `scripts/local_e2e_test.sh`
- Reports:
  - `reports/data_quality_report.md`
  - `reports/local_test_report.md`

## Quick start

```bash
# default: run with Spark in Docker
bash scripts/local_e2e_test.sh

# fallback mode without docker Spark
USE_DOCKER_SPARK=0 bash scripts/local_e2e_test.sh
```

### Full data run (~9.55M rows, 3 months)

```bash
# one-command full local E2E (Kafka + Spark docker)
bash scripts/local_e2e_full_9_5m.sh

# or run manually
bash scripts/download_data.sh
TOPIC=taxi.trip.events.full bash scripts/run_generator_full.sh
TOPIC=taxi.trip.events.full bash scripts/start_streaming_job_docker.sh
python3 jobs/spark_quality_report.py
```

## Notes

- Host Spark may fail with Java 26; this project runs Spark inside Docker (`apache/spark:3.5.1`) to avoid host Java issues.
- Kafka broker for host is `localhost:29092`.
- MinIO is started for local stack completeness, but Spark writes in this MVP to local filesystem.
- Next step: wire Trino + dbt models on top of silver/quarantine tables.
