# NYC Taxi Pipeline — Hướng dẫn chạy

## Yêu cầu

- Docker + Docker Compose
- `kind` CLI (Kubernetes in Docker)
- `kubectl`

---

## 1. Kiến trúc

```
raw parquet ──→ Spark batch ──→ silver/ parquet ──→ Trino ──→ dbt ──→ Mart views
                                 quarantine/ ─────→ Trino ──→ dbt ──→ Invalid table

CDC: Postgres ──→ Debezium ──→ Kafka ──→ cdc-bridge ──→ Kafka topic
```

---

## 2. Chạy local với Docker Compose

### 2.1 Khởi động services

```bash
make up
```

### 2.2 Pipeline đầy đủ

```bash
make verify-all
```

### 2.3 Hoặc từng bước

```bash
make spark-batch       # Batch từ parquet → silver/quarantine
make trino-bootstrap   # Register tables trên Trino
make dbt-build         # Transform + test
make verify-cdc        # CDC: seed → register → bridge
```

### 2.4 Tắt

```bash
make down
```

---

### 2.5 S3-compatible storage với MinIO

MinIO được tích hợp sẵn trong Docker Compose (port `9000` S3 API, `9001` Console). Pipeline có thể chạy với MinIO làm data layer thay cho local filesystem.

**Bật MinIO + upload dữ liệu:**

```bash
make infra-up          # Đảm bảo MinIO đang chạy
make minio-setup       # Tạo buckets + upload raw parquet + lookup CSV
```

**Chạy Spark batch với S3:**

```bash
make spark-batch-s3    # Dùng MONTH=01/02/03 như spark-batch thường
```

**Trino + dbt với S3 data:**

```bash
S3_MODE=true make trino-bootstrap   # Register tables từ S3 paths
make dbt-build                       # dbt không đổi — đọc từ Trino
```

**Spark streaming với S3:**

```bash
make spark-streaming-s3
```

**Kiểm tra dữ liệu trong MinIO:**

```bash
make verify-minio       # Liệt kê buckets + object counts
```

Hoặc mở http://localhost:9001 (user: `minio`, pass: `minio123`).

**Kiến trúc S3 mode:**

```
Raw parquet ─upload─► MinIO (nyc-raw)
                           │
                    Spark ─┤ (s3a://, --s3 flag)
                           │
                    MinIO (nyc-silver, nyc-quarantine, nyc-lookup)
                           │
                    Trino ─┤ (hive.s3.*, S3_MODE=true)
                           │
                    dbt ◄──┘
```

**Lưu ý:**

- `S3_MODE=true` dùng cho `trino-bootstrap`. Khi không set, Trino vẫn đọc từ local FS (mặc định).
- Spark dùng `s3a://` protocol (Hadoop S3A connector). Trino dùng `s3://` protocol.
- Chạy `make spark-batch-s3` yêu cầu `make infra-up` trước đó (network compose để Spark container reach MinIO).

## 3. Chạy trên Kubernetes (kind)

### 3.1 Tạo cluster

Mặc định 3 nodes:

```bash
kind create cluster --config kind.yaml
```

Kiểm tra:

```bash
kubectl cluster-info
kubectl get nodes
```

### 3.2 Chuẩn bị images

Build + load custom images vào kind:

```bash
docker build -f docker/tools.Dockerfile -t nyc-pipeline-tools:k8s .
docker build -f docker/dbt.Dockerfile -t nyc-dbt:k8s .
docker build -f docker/airflow.Dockerfile -t nyc-airflow:k8s .

kind load docker-image nyc-pipeline-tools:k8s nyc-dbt:k8s nyc-airflow:k8s
```

### 3.3 Deploy services

Thứ tự quan trọng (namespace → storage → infra → compute → apps):

```bash
kubectl apply -f k8s/namespace/
kubectl apply -f k8s/storage/
kubectl apply -f k8s/zookeeper/
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/minio/
kubectl apply -f k8s/kafka-ui/
kubectl apply -f k8s/spark/
kubectl apply -f k8s/postgres-cdc/
kubectl apply -f k8s/debezium/
kubectl apply -f k8s/trino/
kubectl apply -f k8s/superset/
kubectl apply -f k8s/airflow/postgres/
kubectl apply -f k8s/airflow/
kubectl apply -f k8s/dbt/
kubectl apply -f k8s/jobs/
```

Kiểm tra:

```bash
kubectl get pods -n nyc-taxi -w
```

### 3.4 Chạy pipeline

Chờ services ổn định, chạy lần lượt:

```bash
# 1. Khởi tạo Postgres + Kafka topics
kubectl apply -f k8s/jobs/postgres-init.yaml -n nyc-taxi
kubectl apply -f k8s/jobs/topic-init.yaml -n nyc-taxi

# 2. CDC — seed data vào Postgres + register Debezium connector
kubectl apply -f k8s/jobs/cdc-seed.yaml -n nyc-taxi
kubectl apply -f k8s/jobs/cdc-register.yaml -n nyc-taxi

# 3. Spark batch — đọc raw parquet → silver/quarantine
kubectl apply -f k8s/jobs/spark-batch.yaml -n nyc-taxi

# 4. Trino bootstrap — register tables từ silver
kubectl apply -f k8s/jobs/trino-bootstrap.yaml -n nyc-taxi

# 5. dbt — transform + test
kubectl apply -f k8s/dbt/job.yaml -n nyc-taxi

# 6. CDC bridge — stream events từ Debezium → Kafka
kubectl apply -f k8s/jobs/cdc-bridge.yaml -n nyc-taxi
```

Kiểm tra kết quả:

```bash
kubectl get jobs -n nyc-taxi
kubectl logs -n nyc-taxi job/<job-name>
```

### 3.5 Smoke test

```bash
kubectl run -n nyc-taxi --rm -i temp --image=nyc-pipeline-tools:k8s \
  --restart=Never -- python3 -c "
from trino.dbapi import connect
cur = connect('svc-trino', 8080, user='test').cursor()
cur.execute('SELECT count(*) FROM hive.nyc.trips')
print('trips:', cur.fetchone()[0])
cur.execute('SELECT count(*) FROM hive.mart.fact_trips')
print('fact_trips:', cur.fetchone()[0])
"
```

### 3.6 Truy cập UI

| Service | URL | Lệnh port-forward |
|---|---|---|
| Kafka UI | http://localhost:38080 | `kubectl port-forward -n nyc-taxi svc/kafka-ui 38080:8080` |
| MinIO | http://localhost:39000 | `kubectl port-forward -n nyc-taxi svc/minio 39000:9000` |
| Superset | http://localhost:38088 | `kubectl port-forward -n nyc-taxi svc/superset 38088:8088` |
| Airflow | http://localhost:38081 | `kubectl port-forward -n nyc-taxi svc/airflow-webserver 38081:8080` |
| Spark master | http://localhost:38082 | `kubectl port-forward -n nyc-taxi svc/spark-master 38082:8080` |
| Trino | `svc-trino:8080` (trong cluster) | — |

Credentials Superset & Airflow: `admin` / `admin`.

### 3.7 Dừng

```bash
# Xoá kind cluster (mất luôn K8s, data PVC hostPath vẫn còn)
kind delete cluster
```

---

## 4. Cấu trúc thư mục

```
k8s/                        # Kubernetes manifests
├── namespace/              # nyc-taxi namespace
├── storage/                # PV + PVC (hostPath trên kind-worker)
├── zookeeper/              # ZK StatefulSet + Service
├── kafka/                  # Kafka StatefulSet + Service
├── kafka-ui/               # Kafka UI Deployment + Service
├── minio/                  # MinIO Deployment + Service + PVC
├── spark/                  # Spark master/worker Deployment + Service
├── postgres-cdc/           # Postgres CDC StatefulSet + Service + PVC
├── debezium/               # Debezium Deployment + Service
├── trino/                  # Trino Deployment + Service + ConfigMap
├── superset/               # Superset Deployment + Service + ConfigMap
├── airflow/                # Airflow (Postgres + webserver + scheduler + init)
├── dbt/                    # dbt Job
└── jobs/                   # One-shot Jobs
    ├── postgres-init.yaml      # Tạo trips table
    ├── topic-init.yaml         # Tạo Kafka topics
    ├── cdc-seed.yaml           # Seed parquet → Postgres (5K rows)
    ├── cdc-register.yaml       # Register Debezium connector
    ├── spark-batch.yaml        # Batch: raw parquet → silver/quarantine
    ├── trino-bootstrap.yaml    # Register tables trên Trino
    └── cdc-bridge.yaml         # CDC events → Kafka topic

docker/                     # Dockerfiles + entrypoint scripts
scripts/                    # Python helper scripts
jobs/                       # Spark job scripts (.py)
dbt/                        # dbt models + tests
data/                       # Raw parquet + lookup (hostPath)
```

## 5. Lưu ý

- **PVC hostPath**: `raw-data-pv` dùng `/mnt/nyc-data` trên `kind-worker`. `project-files-pv` dùng `/mnt/nyc-project`. Khi xoá cluster (`kind delete cluster`), data trên hostPath vẫn còn — cần xoá tay nếu muốn clean.
- **ReadWriteOnce**: Các PVC dùng RWO nên pod phải chạy trên `kind-worker`. Tất cả Job + Trino đã set `nodeSelector` phù hợp trong manifest.
- **Images**: 3 custom images (`nyc-pipeline-tools:k8s`, `nyc-dbt:k8s`, `nyc-airflow:k8s`) cần được build + load vào kind sau mỗi lần tạo cluster.
- **Scripts trên PVC**: `cdc_register_connector.py` và các script khác được mount từ `project-files-pvc`. Nếu sửa script, cần copy vào hostPath: `docker cp <file> kind-worker:/mnt/nyc-project/<path>`.
- **Spark batch**: Chạy `local[*]` trong một Job pod (không dùng cluster mode). Yêu cầu ~4GB RAM. Nếu dữ liệu lớn hơn, tăng resource limits.
