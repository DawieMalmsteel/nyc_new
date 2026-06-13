# 9. Docker Images và Entrypoint Scripts

## 9.1 Tổng quan

Pipeline sử dụng 3 custom Docker images + 1 third-party image cho Spark.

| Image | Dockerfile | Base | Mục đích |
|-------|-----------|------|----------|
| `nyc-pipeline-tools:latest` | `docker/tools.Dockerfile` | Python 3.11-slim | One-shot scripts (topic-init, CDC, Trino, Superset) |
| `nyc-dbt:latest` | `docker/dbt.Dockerfile` | Python 3.11-slim | dbt-trino runner |
| `nyc-airflow:latest` | `docker/airflow.Dockerfile` | apache/airflow:2.10.5 | Airflow webserver + scheduler |
| `apache/spark:3.5.1` | (third-party) | - | Spark master + worker + submit |

---

## 9.2 Tools Image

### Dockerfile
**File**: `docker/tools.Dockerfile`

```dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/project

# CDC bridge/seed dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary sqlalchemy kafka-python trino pandas pyarrow

# Copy all .sh scripts and create symlinks (strip .sh suffix for K8s entrypoints)
COPY docker/*.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh && \
    for f in /usr/local/bin/*.sh; do ln -s "$f" "${f%.sh}"; done

CMD ["bash"]
```

### Packages installed
| Package | Version (mặc định) | Mục đích |
|---------|-------------------|----------|
| `psycopg2-binary` | latest | Kết nối PostgreSQL (CDC seed, init) |
| `sqlalchemy` | latest | ORM cho Postgres insert (CDC seed) |
| `kafka-python` | latest | Kafka consumer/producer (CDC bridge) |
| `trino` | latest | Trino DB-API driver (register, query) |
| `pandas` | latest | DataFrame operations |
| `pyarrow` | latest | Parquet read/write (quality report) |

### Entrypoint Symlinks

Tất cả `.sh` scripts trong `docker/` được copy và tạo symlinks bỏ `.sh`:
```
/usr/local/bin/
├── entrypoint-airflow.sh → entrypoint-airflow
├── entrypoint-cdc-bridge.sh → entrypoint-cdc-bridge
├── entrypoint-cdc-register.sh → entrypoint-cdc-register
├── entrypoint-cdc-seed.sh → entrypoint-cdc-seed
├── entrypoint-dbt.sh → entrypoint-dbt
├── entrypoint-gold-export.sh → entrypoint-gold-export
├── entrypoint-init-postgres.sh → entrypoint-init-postgres
├── entrypoint-quality.sh → entrypoint-quality
├── entrypoint-topic-init.sh → entrypoint-topic-init
├── entrypoint-trino-bootstrap.sh → entrypoint-trino-bootstrap
├── wait-kafka.sh → wait-kafka
```

### Usage examples
```bash
# Container entrypoint
docker run nyc-pipeline-tools entrypoint-topic-init
docker run nyc-pipeline-tools entrypoint-cdc-bridge --bootstrap-server kafka:9092

# K8s pod spec
cmds: ["entrypoint-trino-bootstrap"]
```

---

## 9.3 dbt Image

### Dockerfile
**File**: `docker/dbt.Dockerfile`

```dockerfile
FROM python:3.11-slim

ENV PIP_DEFAULT_TIMEOUT=120

RUN pip install --retries 5 "dbt-trino>=1.7,<2.0"

COPY docker/entrypoint-dbt.sh /usr/local/bin/entrypoint-dbt
RUN chmod +x /usr/local/bin/entrypoint-dbt

WORKDIR /opt/project/dbt
CMD ["bash"]
```

### dbt-trino version range
- `>=1.7, <2.0` — Compatible với Trino 435.
- Cài đặt với `--retries 5` (mạng không ổn định).

### Entrypoint
**File**: `docker/entrypoint-dbt.sh`
```bash
#!/usr/bin/env bash
set -euo pipefail
TRINO_HOST="${TRINO_HOST:-trino-coordinator}"
TRINO_PORT="${TRINO_PORT:-8080}"

# Wait for Trino
for i in {1..60}; do
  if curl -sf "http://${TRINO_HOST}:${TRINO_PORT}/v1/info" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Sync partitions trước khi build
cd /opt/project
python3 /opt/project/scripts/trino_sync_partitions.py

cd /opt/project/dbt
exec dbt build "$@"
```

---

## 9.4 Airflow Image

### Dockerfile
**File**: `docker/airflow.Dockerfile`

```dockerfile
FROM apache/airflow:2.10.5-python3.11

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=False

USER airflow

ARG AIRFLOW_VERSION=2.10.5

RUN pip install --no-cache-dir --no-deps \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-cncf-kubernetes==8.4.2" \
    "apache-airflow-providers-docker==3.14.1" \
    "apache-airflow-providers-http==5.3.0" \
    "apache-airflow-providers-postgres==6.2.0" \
    "apache-airflow-providers-common-sql==1.27.0" \
    "apache-airflow-providers-trino==6.2.0" \
    && pip install --no-cache-dir \
    requests lz4 orjson trino==0.337.0 kubernetes==29.0.0
```

### Providers

| Provider | Version | Mục đích |
|----------|---------|----------|
| `cncf-kubernetes` | 8.4.2 | KubernetesPodOperator (quan trọng nhất) |
| `docker` | 3.14.1 | Docker Operator |
| `http` | 5.3.0 | HTTP requests |
| `postgres` | 6.2.0 | Postgres connection |
| `common-sql` | 1.27.0 | SQL utilities |
| `trino` | 6.2.0 | Trino connection |

### Packages

| Package | Mục đích |
|---------|----------|
| `kubernetes==29.0.0` | K8s client (V1Volume, V1VolumeMount...) |
| `trino==0.337.0` | Trino DB-API driver |
| `requests` | HTTP requests |
| `lz4, orjson` | Compression/parsing |

### Entrypoint
**File**: `docker/entrypoint-airflow.sh`

```bash
case "${AIRFLOW_ROLE:-webserver}" in
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  init)
    airflow db migrate
    airflow users create \
      --username admin --firstname Admin \
      --lastname User --email admin@local \
      --password admin --role Admin || true
    echo "[airflow-init] complete"
    ;;
esac
```

---

## 9.5 Spark Image (Third-party)

Image: `apache/spark:3.5.1`

**Không custom image** — dùng trực tiếp từ Docker Hub.
Cấu hình S3A và Kafka packages qua `--packages` CLI argument:
```bash
# S3A packages (cho MinIO)
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

# Spark-Kafka package (cho streaming)
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

---

## 9.6 Entrypoint Scripts Reference

### wait-kafka.sh
```bash
# TCP wait script — chờ Kafka broker accept connections
wait-kafka <bootstrap-server>  # default: kafka:9092
# Timeout: 120s
```

### entrypoint-topic-init.sh
```bash
# Wait Kafka → create topics
wait-kafka svc-kafka:9092
python3 /opt/project/scripts/create_kafka_topics.py \
  --bootstrap-server svc-kafka:9092
```

### entrypoint-init-postgres.sh
```bash
# Wait Postgres → create trips table (idempotent)
# Dùng Python psycopg2 (không cần psql)
```

### entrypoint-cdc-seed.sh
```bash
# Wait Postgres → seed data từ Parquet
wait postgres ready (psycopg2 connect)
exec python3 /opt/project/scripts/cdc_seed.py "$@"
```

### entrypoint-cdc-register.sh
```bash
# Register Debezium connector
exec python3 /opt/project/scripts/cdc_register_connector.py "$@"
```

### entrypoint-cdc-bridge.sh
```bash
# Wait Kafka → run bridge
wait-kafka svc-kafka:9092 60
exec python3 /opt/project/scripts/cdc_bridge.py "$@"
```

### entrypoint-trino-bootstrap.sh
```bash
# Register Hive tables
python3 /opt/project/scripts/trino_register.py
```

### entrypoint-dbt.sh
```bash
# Wait Trino → sync partitions → dbt build
```

### entrypoint-quality.sh
```bash
# Generate quality report
python3 /opt/project/jobs/spark_quality_report.py
```

---

## 9.7 Image Build

### Kubernetes (Skaffold) ⭐

Skaffold tự động build images qua `build.artifacts`:
```yaml
# skaffold.yaml
build:
  local:
    push: false  # không push registry, dùng local kind
  artifacts:
    - image: nyc-pipeline-tools
      docker: { dockerfile: docker/tools.Dockerfile }
    - image: nyc-dbt
      docker: { dockerfile: docker/dbt.Dockerfile }
    - image: nyc-airflow
      docker: { dockerfile: docker/airflow.Dockerfile }
```

Chạy `skaffold dev` → tự động build + load vào kind cluster.

### Build thủ công (cho debug)
```bash
docker build -f docker/tools.Dockerfile -t nyc-pipeline-tools:k8s .
docker build -f docker/dbt.Dockerfile -t nyc-dbt:k8s .
docker build -f docker/airflow.Dockerfile -t nyc-airflow:k8s .
kind load docker-image nyc-pipeline-tools:k8s nyc-dbt:k8s nyc-airflow:k8s
```

### Docker Compose (Legacy)
```bash
docker compose build tools  # tools image
docker build -f docker/dbt.Dockerfile -t nyc-dbt:latest .
docker build -f docker/airflow.Dockerfile -t nyc-airflow:latest .
```
