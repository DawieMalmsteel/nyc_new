# NYC Taxi Data Pipeline — Tài Liệu Hoàn Chỉnh

> **Phiên bản**: Kubernetes (kind) + Skaffold ⭐ (primary) | Docker Compose (legacy)  
> **Ngày cập nhật**: 2026-06-12  
> **Ngôn ngữ**: Tiếng Việt  

---

## Mục Lục

1. [Tổng Quan Kiến Trúc NYC Taxi Data Pipeline](#1-tổng-quan-kiến-trúc-nyc-taxi-data-pipeline)
2. [Hướng Dẫn Triển Khai](#2-hướng-dẫn-triển-khai)
3. [Xử Lý Dữ Liệu với Apache Spark](#3-xử-lý-dữ-liệu-với-apache-spark)
4. [dbt Models và Data Transformation](#4-dbt-models-và-data-transformation)
5. [Trino Catalog và Hive Metadata](#5-trino-catalog-và-hive-metadata)
6. [Airflow DAGs — Pipeline Orchestration](#6-airflow-dags--pipeline-orchestration)
7. [CDC Pipeline — Change Data Capture](#7-cdc-pipeline--change-data-capture)
8. [Apache Superset — Dashboard và Visualization](#8-apache-superset--dashboard-và-visualization)
9. [Docker Images và Entrypoint Scripts](#9-docker-images-và-entrypoint-scripts)
10. [Helm Chart và Skaffold Deployment](#10-helm-chart-và-skaffold-deployment)
11. [Scripts và Tiện Ích](#11-scripts-và-tiện-ích)
12. [Luồng Dữ Liệu và Storage](#12-luồng-dữ-liệu-và-storage)
13. [Tổng Kết Kiến Trúc](#13-tổng-kết-kiến-trúc)
14. [Biểu Đồ Kiến Trúc Skaffold, Helm và Kubernetes](#14-biểu-đồ-kiến-trúc-skaffold-helm-và-kubernetes)

---

# 1. Tổng Quan Kiến Trúc NYC Taxi Data Pipeline

## 1.1 Giới thiệu dự án

Dự án **NYC Taxi Data Pipeline** là một hệ thống xử lý dữ liệu *batch* và *streaming* end-to-end, 
xây dựng trên dữ liệu chuyến đi taxi thực tế từ **NYC TLC (Taxi & Limousine Commission)**. 
Hệ thống xử lý khoảng **10.2 triệu chuyến đi hợp lệ** và **1.07 triệu bản ghi lỗi**, 
với kiến trúc hiện đại sử dụng **Spark**, **Kafka**, **MinIO S3**, **Trino**, **dbt**, **Apache Superset** 
và được điều phối bởi **Apache Airflow**.

### Mục tiêu

- **Xử lý tin cậy**: Dữ liệu thô (raw Parquet) được enrich, validate và chia thành luồng valid/quarantine
- **Batch + Streaming**: Hai chế độ xử lý bổ trợ lẫn nhau — backfill lịch sử + streaming real-time
- **Data Pipeline hiện đại**: Lakehouse architecture với S3 làm storage layer
- **Phân tích kinh doanh**: 15 dbt models, 4 charts, dashboard Superset, 10 câu hỏi analytics
- **CDC (Change Data Capture)**: Hỗ trợ ingest từ Postgres qua Debezium như một nguồn sự kiện thay thế

### Chế độ triển khai chính

> 🚀 **Skaffold** là công cụ triển khai **chính thức** và **ưu tiên**.
> `skaffold dev --namespace nyc-taxi` — một lệnh duy nhất cho toàn bộ pipeline.
> Docker Compose (Makefile) chỉ dùng cho debug/test local nhanh.

---

## 1.2 Kiến trúc tổng thể

### Sơ đồ luồng dữ liệu (Kubernetes — Skaffold)

```mermaid
flowchart TD
    subgraph SOURCE["📥 Nguồn dữ liệu"]
        RP["Raw Parquet<br/>NYC TLC"]
        K1["Kafka<br/>taxi.trip.events"]
        PG["Postgres WAL"]
    end

    subgraph CDC_LINK["CDC Bridge"]
        BR["cdc_bridge.py<br/>unwrap + transform"]
        DZ["Debezium 2.5<br/>Kafka Connect"]
    end

    subgraph PROCESS["⚙️ Xử lý"]
        SB["Spark Batch<br/>local[*]"]
        SS["Spark Streaming<br/>Kafka consumer"]
    end

    subgraph MINIO["💾 MinIO S3 Storage"]
        RAW["nyc-raw<br/>153MB"]
        SILVER["nyc-silver<br/>265MB"]
        QUAR["nyc-quarantine<br/>36MB"]
        LOOKUP["nyc-lookup<br/>12KB"]
    end

    subgraph CATALOG["🗄️ Catalog & Transform"]
        TRINO["Trino 435<br/>Hive catalog"]
        DBT["dbt-trino<br/>15 models, 9 tests"]
    end

    subgraph VIS["📊 Visualization"]
        SUP["Apache Superset 4.0.0<br/>7 datasets, 4 charts"]
    end

    subgraph ORCH["🎯 Orchestration"]
        AIR["Airflow 2.10.5<br/>3 DAGs"]
    end

    RP -->|minio-setup job| RAW
    RAW --> SB
    K1 --> SS
    PG --> DZ --> BR --> K1
    SB --> SILVER
    SB --> QUAR
    SS --> SILVER
    SILVER --> TRINO
    LOOKUP --> TRINO
    TRINO --> DBT
    DBT --> SUP
    AIR -.-> SB & SS & TRINO & DBT & SUP
```

### Luồng xử lý chi tiết

#### 1. Batch Path (Backfill lịch sử)

```mermaid
flowchart LR
    RP["Raw Parquet NYC TLC"] -->|minio-setup| MINIO["MinIO nyc-raw/"]
    MINIO --> SPARK["Spark Batch<br/>spark_local_batch.py"]
    SPARK --> ENRICH["Enrich:<br/>cast types, join zones,<br/>trip_id, metadata"]
    ENRICH --> VAL["Validate:<br/>10 rules"]
    VAL -->|valid| SIL["nyc-silver/trips/<br/>(partitioned)"]
    VAL -->|invalid| QUAR["nyc-quarantine/"]
```

#### 2. Streaming Path (Real-time)

```mermaid
flowchart LR
    PROD["Kafka Producer"] --> TOPIC["taxi.trip.events"]
    TOPIC --> SS["Spark Streaming<br/>spark_stream_taxi_events.py"]
    SS --> SAME["Enrich + Validate<br/>(same logic)"]
    SAME -->|foreachBatch| SIL2["nyc-silver/trips/<br/>append"]
```

#### 3. CDC Path (Change Data Capture)

```mermaid
flowchart LR
    PG["Postgres 16<br/>WAL logical"] --> DEB["Debezium 2.5<br/>Kafka Connect"]
    DEB --> CDC_TOPIC["nyc_cdc.public.trips"]
    CDC_TOPIC --> BRIDGE["cdc_bridge.py<br/>unwrap + transform"]
    BRIDGE --> TOPIC2["taxi.trip.events"]
    TOPIC2 --> SS2["Spark Streaming"]
```

---

## 1.3 Công nghệ sử dụng

| Layer | Công nghệ | Phiên bản | Vai trò |
|-------|-----------|-----------|---------|
| **Storage** | MinIO S3 | latest | Lưu trữ object: raw, silver, quarantine, lookup |
| **Processing (batch)** | Apache Spark | 3.5.1 | Enrich + Validate + Write Parquet |
| **Processing (stream)** | Apache Spark + Kafka | 3.5.1 | Streaming consume + micro-batch |
| **Messaging** | Apache Kafka + ZK | 7.6.1 | Event bus cho streaming path |
| **SQL Catalog** | Trino (Hive connector) | 435 | SQL engine đọc Parquet từ S3 |
| **Transformations** | dbt-trino | ≥1.7, <2.0 | 15 models (staging → marts → gold) |
| **Visualization** | Apache Superset | 4.0.0 | Dashboard + charts |
| **Orchestration** | Apache Airflow | 2.10.5 | 3 DAGs, KubernetesPodOperator |
| **CDC** | Debezium + Postgres | 2.5 / 16 | WAL-based CDC |
| **Container** | Docker / kind | - | Container hóa toàn bộ |
| **Deploy** ⭐ | **Skaffold + Helm** | **v4beta3** | **Build + Deploy + Sync + Port-forward (primary)** |

### Kết nối giữa các dịch vụ (Kubernetes)

```
Spark → MinIO:    s3a:// (S3A Hadoop connector) với endpoint http://svc-minio:9000
Trino → MinIO:    s3://   (Hive S3 connector) với endpoint http://svc-minio:9000
Spark → Kafka:    svc-kafka:9092 (⚠️ prefix 'svc-' bắt buộc trong K8s)
Trino ← dbt:      svc-trino:8080 (Trino JDBC)
Superset → Trino: trino://analytics@svc-trino:8080/hive/mart (SQLAlchemy)
Airflow → Spark:  KubernetesPodOperator (pod tạm với image apache/spark:3.5.1)
```

---

## 1.4 Cấu trúc thư mục

```
nyc_new/
├── airflow/dags/           # 3 Airflow DAGs (orchestration)
│   ├── nyc_e2e_pipeline.py
│   ├── nyc_cdc_pipeline.py
│   └── nyc_analytics_refresh.py
├── charts/nyc-taxi/        # Helm chart cho K8s deployment
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/          # 30+ K8s manifest templates
├── config/pipeline.yml     # Pipeline configuration
├── data/                   # Data lake (gitignored)
│   ├── raw/                # Raw Parquet files
│   ├── silver/             # Enriched valid trips
│   ├── quarantine/         # Invalid trips
│   ├── lookup/             # Zone lookup CSV
│   └── checkpoints/        # Streaming checkpoints
├── dbt/                    # dbt-trino transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/             # 15 SQL models
│   │   ├── staging/        # 3 models (stg_trips, stg_zones, stg_invalid_trips)
│   │   ├── marts/          # 8 models (fact_trips, dim_zone, ...)
│   │   └── gold/           # 4 models (gold_fact_trips, ...)
│   └── tests/              # 4 test files (9 tests)
├── docker/                 # Dockerfiles + entrypoint scripts
│   ├── tools.Dockerfile    # Base Python 3.11 image
│   ├── dbt.Dockerfile      # dbt-trino image
│   ├── airflow.Dockerfile  # Airflow 2.10.5 image
│   ├── *.sh                # Entrypoint scripts (trino-bootstrap, dbt, CDC...)
│   ├── superset/           # Superset config + bootstrap
│   └── trino/              # Trino config (catalog, properties)
├── docker-compose.yml      # 16+ services, 6 profiles
├── jobs/                   # Spark processing jobs
│   ├── spark_local_batch.py       # Batch processor (main)
│   ├── spark_stream_taxi_events.py # Kafka streaming processor
│   ├── spark_batch_backfill.py     # Legacy placeholder
│   ├── spark_quality_report.py     # Quality report (PyArrow)
│   └── kafka_stream_processor.py   # Python-only stream processor (alternative)
├── k8s/                    # Legacy K8s manifests (raw YAML)
├── scripts/                # Utility scripts
│   ├── trino_register.py   # Register Hive tables
│   ├── cdc_bridge.py       # CDC → standard format bridge
│   ├── cdc_seed.py         # Seed Postgres from Parquet
│   ├── cdc_register_connector.py  # Register Debezium connector
│   ├── superset_bootstrap.py      # Idempotent Superset setup
│   ├── run_analytics_questions.py # 10 analytics queries
│   ├── verify_mart.py      # Row count verification
│   ├── export_gold_to_minio.py    # Export gold datasets
│   └── k8s_ui.sh           # Port-forward manager
├── sql/                    # SQL queries
│   ├── analytics_questions.sql  # 10 business questions
│   └── smoke_tests.sql         # Simple smoke tests
├── terraform/              # Terraform for MinIO buckets
├── skaffold.yaml           # Skaffold config (K8s deploy)
├── Makefile                # 40+ targets (Docker Compose mode)
├── kind.yaml               # kind cluster config (3 nodes)
└── AGENTS.md               # Full project guidelines
```

---

## 1.5 Validation Rules (Spark Batch & Streaming)

Pipeline áp dụng 10 luật validation đồng nhất trên cả batch và streaming:

| # | Rule | Error Message | Điều kiện lỗi |
|---|------|---------------|----------------|
| 1 | Event ID null | `event_id_null` | `event_id` is null (streaming only) |
| 2 | Pickup datetime | `pickup_datetime_null_or_invalid` | `pickup_ts` is null |
| 3 | Dropoff datetime | `dropoff_datetime_null_or_invalid` | `dropoff_ts` is null |
| 4 | Trip duration | `invalid_trip_duration` | `dropoff_ts <= pickup_ts` |
| 5 | Trip distance | `trip_distance_must_be_gt_0` | `trip_distance <= 0` |
| 6 | Fare amount | `fare_amount_must_be_gte_0` | `fare_amount < 0` |
| 7 | Total vs Fare | `total_amount_must_be_gte_fare_amount` | `total_amount < fare_amount` |
| 8 | Passenger count | `passenger_count_out_of_range` | `passenger_count < 1 or > 6` |
| 9 | Payment type | `payment_type_out_of_range` | `payment_type < 1 or > 6` |
| 10 | Pickup/Dropoff zone | `pickup/dropoff_location_not_found` | Zone ID không có trong lookup |

Dữ liệu hợp lệ → `nyc-silver/trips/pickup_year=*/pickup_month=*/`
Dữ liệu không hợp lệ → `nyc-quarantine/invalid_trips/` (kèm danh sách lỗi)

---

## 1.6 Chế độ triển khai

> ⚠️ **Skaffold (Kubernetes/kind) là chế độ triển khai chính.**
> Docker Compose (Makefile) chỉ là phương án dự phòng cho debug local nhanh.

| Chế độ | Công cụ | Cluster | Mục đích |
|--------|---------|---------|----------|
| 🚀 **Kubernetes (kind)** ⭐ | **Skaffold + Helm** | 3 nodes (kind) | **Primary** — build, auto-sync, port-forward, orchestration |
| 🐳 Docker Compose | Make + Docker Compose | Docker host | Legacy — debug/test nhanh |

### Tại sao Skaffold là chính?

| Tính năng | Skaffold (K8s) | Docker Compose (Makefile) |
|-----------|----------------|---------------------------|
| **Orchestration** | ✅ Airflow 3 DAGs tự động | ❌ Make targets thủ công |
| **Auto-rebuild** | ✅ Dockerfile changes → rebuild | ❌ Build lại thủ công |
| **File sync** | ✅ Watch + auto-sync → PVC | ✅ Bind mount trực tiếp |
| **Port-forward** | ✅ Tự động 39080-39087 | ❌ Published ports cố định |
| **Hot-reload** | ✅ Skaffold sync → file-sync pod | ✅ Bind mount |
| **Production-like** | ✅ K8s native | ❌ Docker host đơn |
| **Tính năng** | ✅ Airflow, CDC, gold export | ✅ Cơ bản |
| **Resources** | 3 node cluster, ~12GB RAM | Single host, ~8GB RAM |

---

## 1.7 Dữ liệu tham chiếu

### Zone Lookup (taxi_zone_lookup.csv)
- 265 zones (261 distinct sau join)
- Các trường: `LocationID`, `Borough`, `Zone`, `service_zone`
- 7 Boroughs: Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, Unknown

### Payment Types
| code | Description |
|------|-------------|
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |

### Vendor IDs
| ID | Name |
|----|------|
| 1 | Creative Mobile |
| 2 | VeriFone |

### Rate Codes
| ID | Description |
|----|-------------|
| 1 | Standard |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau/Westchester |
| 5 | Negotiated |
| 6 | Group ride |

---

## 1.8 Kết quả xử lý (Kubernetes — Skaffold mode)

| Metric | Giá trị | Ghi chú |
|--------|---------|---------|
| Valid trips | **10,188,983** | Dữ liệu 2002-2024 |
| Invalid trips | **1,074,370** | ~11.24% invalid rate |
| Zone lookup | **265** | Taxi zones |
| dbt tests | **24/24 PASS** | 15 models + 9 tests |
| Analytics | **10/10 PASS** | 10 business SQL queries |
| CDC bridge throughput | **~445 ev/s** | Async mode |
| Spark runtime (3 months) | **~9 min** | local[*] mode |
| Pipeline UIs | **8 services** | 39080-39087 port-forwards |



# 2. Hướng Dẫn Triển Khai

> 🚀 **Skaffold + Kubernetes (kind) là chế độ triển khai chính.**
> Docker Compose (Makefile) chỉ dùng cho debug/test local nhanh.

---

## 2.1 Yêu cầu hệ thống

### Kubernetes (kind) Mode ⭐ (Primary)

| Requirement | Version / Chi tiết |
|-------------|-------------------|
| **Docker** | Engine ≥ 24.0 |
| **kind** | ≥ 0.20 (Kubernetes in Docker) |
| **kubectl** | ≥ 1.28 |
| **Skaffold** | ≥ 2.10 (Skaffold v2) |
| **Helm** | ≥ 3.0 |
| **Make** | GNU Make ≥ 4.0 |
| **Disk** | ~10GB (images + data + cluster) |
| **RAM** | ~12GB (3 node cluster) |
| **Python** | 3.11+ (optional, cho scripts verify local) |

### Docker Compose Mode (Secondary)
| Requirement | Chi tiết |
|-------------|----------|
| Docker Engine ≥ 24.0 + Docker Compose | ~5GB disk, ~8GB RAM |

### Port Requirements

| Port | Service | Ghi chú |
|------|---------|---------|
| **39080-39087** | Tất cả services | **Kubernetes** (Skaffold port-forward) |
| 38080-38088, 39000 | kind NodePort | Cluster internal (không cần dùng trực tiếp) |
| 8088, 9000-9001, 8083, 8085, 8080-8084 | Docker Compose | Published ports |

---

## 2.2 Triển khai với Skaffold (Kubernetes/kind) ⭐

### 2.2.1 Luồng triển khai tổng quan

```mermaid
flowchart TD
    PREREQ["1. Prerequisites<br/>kind cluster exits?<br/>→ Nếu chưa: kind create cluster"]
    SKDEV["2. skaffold dev --namespace nyc-taxi"]
    BUILD["Build 3 artifacts<br/>• nyc-pipeline-tools<br/>• nyc-dbt<br/>• nyc-airflow"]
    PRE_HOOK["Pre-deploy hook<br/>• Xoá immutable jobs<br/>• Sync files → PVC"]
    HELM["Helm install<br/>30+ K8s resources"]
    POST_HOOK["Post-deploy hook<br/>• Hiển thị URLs<br/>• Mở browser"]
    PF["Port-forward<br/>39080-39087"]
    WATCH["Watch mode<br/>File changes → auto-sync to PVC"]
    JOBS["3. Setup jobs (~2-3 phút)<br/>• topic-init → Kafka ready<br/>• postgres-init → PG ready<br/>• minio-setup → buckets + data"]
    AIRFLOW["4. Airflow tự động<br/>• e2e_pipeline (@monthly)<br/>• cdc_pipeline (manual)<br/>• analytics_refresh (@weekly)"]
    VERIFY["5. Kiểm tra<br/>• UIs (39080-39087)<br/>• verify-mart<br/>• verify-analytics"]

    PREREQ --> SKDEV
    SKDEV --> BUILD --> PRE_HOOK --> HELM --> POST_HOOK --> PF --> WATCH
    WATCH -.->|loop| BUILD
    POST_HOOK --> JOBS --> AIRFLOW --> VERIFY
```

### 2.2.2 Prerequisites — Tạo kind cluster

```bash
# Tạo cluster nếu chưa có (3 nodes: 1 control-plane + 2 workers)
kind create cluster --config kind.yaml

# Kiểm tra
kubectl cluster-info
kubectl get nodes

# Output:
# NAME                 STATUS   ROLES           AGE
# kind-control-plane   Ready    control-plane   2m
# kind-worker          Ready    <none>          1m
# kind-worker2         Ready    <none>          1m
```

**Cluster config (`kind.yaml`):**

| Node | Role | extraPortMappings | extraMounts |
|------|------|-------------------|-------------|
| control-plane | kube-apiserver, etcd, scheduler | 38080→30080, 38081→30081, 38082→30082, 38088→30088, 39000→39000 | - |
| kind-worker | Worker (nodeSelector target) | - | `/mnt/nyc-project`, `/mnt/nyc-data` |
| kind-worker2 | Worker (spare) | - | `/mnt/nyc-project`, `/mnt/nyc-data` |

### 2.2.3 Deploy toàn bộ pipeline

```bash
# ⭐ LỆNH CHÍNH — một lệnh duy nhất cho mọi thứ
skaffold dev --namespace nyc-taxi

# Nếu không cần watch mode (deploy một lần)
skaffold run --namespace nyc-taxi

# Build images only (không deploy)
skaffold build --namespace nyc-taxi

# Xoá tài nguyên đã deploy
skaffold delete --namespace nyc-taxi
```

### 2.2.4 Đợi jobs hoàn thành

Sau khi `skaffold dev` chạy, đợi các setup jobs:

```bash
# Kiểm tra tất cả pods
kubectl get pods -n nyc-taxi -w

# Đợi topic-init (tạo Kafka topics)
kubectl wait --for=condition=complete job -n nyc-taxi topic-init --timeout=120s

# Đợi postgres-init (tạo trips table)
kubectl wait --for=condition=complete job -n nyc-taxi postgres-init --timeout=120s

# Đợi minio-setup (upload raw data)
kubectl wait --for=condition=complete job -n nyc-taxi minio-setup --timeout=180s

# Kiểm tra tất cả
kubectl get pods -n nyc-taxi
# Kỳ vọng: tất cả Running hoặc Completed
```

### 2.2.5 UIs & Port-Forwards

Skaffold tự động port-forward từ services → localhost:39080-39087.

| Service | URL | Port | Credentials |
|---------|-----|------|-------------|
| **Apache Superset** | http://localhost:39080 | 39080 | `admin` / `admin` |
| MinIO S3 API | http://localhost:39081 | 39081 | `minio` / `minio123` |
| **Kafka UI** | http://localhost:39082 | 39082 | — |
| Spark Master | http://localhost:39083 | 39083 | — |
| Trino | http://localhost:39084 | 39084 | — |
| **Airflow** | http://localhost:39085 | 39085 | `admin` / `admin` |
| MinIO Console | http://localhost:39086 | 39086 | `minio` / `minio123` |
| Postgres CDC | `localhost:39087` | 39087 | `postgres` / `postgres` |

Nếu không dùng skaffold, chạy port-forward thủ công:
```bash
make k8s-ui
# hoặc
./scripts/k8s_ui.sh start
```

### 2.2.6 Kích hoạt Airflow DAGs

Pipeline tự động chạy theo lịch. Để trigger thủ công:

```bash
# 1. Qua Airflow Web UI
#    http://localhost:39085 → admin/admin → unpause DAG → Trigger

# 2. Qua CLI
# nyc_e2e_pipeline (Spark → Trino → dbt → Superset → analytics)
kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_e2e_pipeline

# nyc_cdc_pipeline (Seed Postgres → Debezium → Bridge)
kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_cdc_pipeline

# nyc_analytics_refresh (dbt → Superset → analytics check)
kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_analytics_refresh
```

### 2.2.7 Verify pipeline

```bash
# Row counts qua Trino (dim_zone, fact_trips, mart_hourly, mart_revenue)
make k8s-verify

# 10 analytics SQL queries (kỳ vọng PASS 10/10)
make k8s-verify-analytics

# CDC pipeline check
make k8s-verify-cdc

# Pod status
make k8s-status

# Logs của job cụ thể
make k8s-logs JOB=spark-batch

# Xem trực tiếp logs pod
kubectl logs -n nyc-taxi job/spark-batch --follow
```

### 2.2.8 Xem logs & debug

```bash
# Tất cả pods
kubectl get pods -n nyc-taxi

# Logs của pod
kubectl logs -n nyc-taxi -l app=trino --tail=50
kubectl logs -n nyc-taxi -l app=superset --tail=50
kubectl logs -n nyc-taxi -l app=airflow-webserver --tail=50

# Logs của job (sau khi hoàn thành)
kubectl logs -n nyc-taxi job/minio-setup
kubectl logs -n nyc-taxi job/postgres-init
kubectl logs -n nyc-taxi job/topic-init

# Describe pod để xem lỗi
kubectl describe pod -n nyc-taxi -l app=kafka
```

### 2.2.9 Cleanup & Destroy

```bash
# Skaffold dev: nhấn Ctrl+C để dừng (giữ cluster + data)

# Scale down services (giữ dữ liệu)
make k8s-stop

# Clean MinIO data (xóa silver/quarantine)
make k8s-clean

# Destroy cluster (🔥 mất HẾT dữ liệu)
kind delete cluster --name kind
# hoặc
make k8s-destroy
```

---

## 2.3 Cấu hình chi tiết Skaffold

### 2.3.1 Build artifacts

```yaml
# skaffold.yaml
build:
  local:
    push: false  # Không push lên registry — dùng local kind cluster
  artifacts:
    - image: nyc-pipeline-tools    # tools.Dockerfile
      sync:
        manual:
          - src: "airflow/dags/**/*.py" → dest: /opt/project/airflow/dags/
          - src: "jobs/**/*"            → dest: /opt/project/jobs/
          - src: "scripts/**/*"         → dest: /opt/project/scripts/
          - src: "dbt/**/*"             → dest: /opt/project/dbt/
          - src: "charts/**/*"          → dest: /opt/project/charts/
    - image: nyc-dbt                   # dbt.Dockerfile
    - image: nyc-airflow               # airflow.Dockerfile
```

### 2.3.2 Deploy hooks

**Pre-deploy hook** (chạy trước Helm install):
```bash
# 1. Xoá immutable jobs (Jobs không thể update)
kubectl delete job -n nyc-taxi --all --ignore-not-found

# 2. Sync project files → kind-worker PVC
tar cf - airflow/dags/ jobs/ scripts/ dbt/ charts/ \
  | docker exec -i kind-worker tar xf - -C /mnt/nyc-project
```

**Post-deploy hook** (chạy sau Helm install):
```bash
# Hiển thị URLs
echo "Superset  http://localhost:39080  admin/admin"
echo "Airflow   http://localhost:39085  admin/admin"
# Mở browser tabs
xdg-open http://localhost:39085 2>/dev/null || true
```

### 2.3.3 Sync rules (hot-reload)

Khi file thay đổi trong quá trình `skaffold dev`:
```
Local file change
    → Skaffold detects change
    → Push file to file-sync pod (trong K8s)
    → file-sync pod ghi vào PVC tại /opt/project/
    → Tất cả pods mount PVC thấy file mới ngay lập tức
```

### 2.3.4 Port-forward config

```yaml
portForward:
  - resourceType: service
    resourceName: svc-superset
    port: 8088
    localPort: 39080
  - resourceType: service
    resourceName: svc-minio
    port: 9000
    localPort: 39081
  # ... 8 services total: 39080-39087
```

---

## 2.4 CDC Pipeline (qua Skaffold)

### 2.4.1 Kiến trúc CDC

```
Postgres 16 (WAL logical replication)
    → Debezium 2.5 connector (Kafka Connect)
    → Raw topic: nyc_cdc.public.trips
    → cdc_bridge.py (unwrap + transform) ← Airflow DAG Task
    → Standard topic: taxi.trip.events
    → Spark Streaming (cùng logic batch) ← Airflow DAG Task
```

### 2.4.2 Kích hoạt CDC

CDC pipeline chạy qua Airflow DAG `nyc_cdc_pipeline` (3 tasks):

```bash
# Trigger DAG
kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_cdc_pipeline
```

**3 tasks trong DAG:**
1. `cdc_seed` — Đọc 5000 rows từ Parquet, insert vào Postgres
2. `cdc_register` — Đăng ký Debezium connector
3. `cdc_bridge` — Bridge CDC events → taxi.trip.events (async, ~445 ev/s)

### 2.4.3 Verify CDC

```bash
make k8s-verify-cdc
# hoặc từng bước:
kubectl exec -n nyc-taxi statefulset/postgres-cdc -- \
  psql -U postgres -d nyc_taxi -c "SELECT count(*) FROM trips;"
```

---

## 2.5 Khắc phục sự cố thường gặp (Skaffold/K8s)

| Vấn đề | Nguyên nhân | Giải pháp |
|--------|-------------|-----------|
| **Namespace stuck "Terminating"** | Finalizers không được gỡ | `kubectl replace --raw /api/v1/namespaces/nyc-taxi/finalize -f ...` |
| **Spark S3A fails** | Thiếu `--packages` trên CLI | Dùng `--packages` trên spark-submit, không dùng `spark.jars.packages` |
| **Spark Ivy cache lỗi** | Permissions | `chmod -R 777 /opt/project/.ivy2/` |
| **MinIO commit lỗi** | S3 không support atomic rename | Set `--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` |
| **dbt build fails** | Dùng `materialized='table'` | Tất cả models phải là `materialized='view'` |
| **Kafka connection fails** | Sai service name | Dùng `svc-kafka:9092` (⚠️ không phải `kafka:9092`) |
| **Trino không thấy partitions** | Metadata chưa sync | `CALL hive.system.sync_partition_metadata(...)` |
| **File-sync không hoạt động** | Skaffold không chạy | Run `skaffold dev` hoặc sync thủ công |
| **Airflow pod lỗi** | Thiếu service account | Kiểm tra `airflow-sa` + Role + RoleBinding |

### PVC Sync thủ công (khi không dùng skaffold)

```bash
cd /home/dwcks/vsf_gsm/nyc_new
tar cf - \
  --exclude='dbt/logs' --exclude='dbt/target' --exclude='.git' \
  --exclude='__pycache__' --exclude='*.pyc' --exclude='*.pyo' \
  airflow/dags/ jobs/ scripts/ dbt/ charts/ \
  | docker exec -i kind-worker tar xf - -C /mnt/nyc-project
```

---

## 2.6 Docker Compose Mode (Legacy — Debug/Test nhanh)

> ⚠️ Chỉ dùng Docker Compose khi cần debug nhanh trên máy local.
> Mọi triển khai chính thức đều dùng Skaffold.

### 2.6.1 Quick Start

```bash
# 1. Khởi động core services
make infra-up

# 2. Chuẩn bị dữ liệu
make kafka-topics
make minio-setup

# 3. Chạy Spark batch
make spark-batch

# 4. Trino + dbt
make trino-bootstrap
make dbt-build

# 5. Superset
make superset-bootstrap

# 6. Kiểm tra
make verify-mart
make verify-analytics
```

### 2.6.2 Makefile Targets

| Target | Mô tả |
|--------|-------|
| `infra-up` | Start core (ZK, Kafka, MinIO, Spark) |
| `infra-up-all` | Start tất cả (Trino, dbt, Superset, Airflow) |
| `spark-batch` | Batch backfill (MONTH=03) |
| `trino-bootstrap` | Register Hive tables |
| `dbt-build` | dbt models + tests |
| `superset-bootstrap` | Register datasets, charts, dashboard |
| `verify-all` | Full pipeline verification |

---

## 2.7 So sánh Deployment: Skaffold vs Docker Compose

| Khía cạnh | 🚀 Skaffold (K8s) ⭐ | 🐳 Docker Compose |
|-----------|----------------------|-------------------|
| **Lệnh deploy** | `skaffold dev` | `make infra-up-all` |
| **Build** | Tự động (Skaffold artifacts) | Thủ công (docker compose build) |
| **File sync** | Auto sync → PVC (hot-reload) | Bind mount trực tiếp |
| **Orchestration** | Airflow 3 DAGs (tự động) | Make targets (thủ công) |
| **Port-forward** | Tự động 39080-39087 | Published ports (8088, 9000...) |
| **Production-like** | ✅ K8s native | ❌ Docker host đơn |
| **Tính năng** | ✅ Airflow, CDC, gold export | ✅ Cơ bản |
| **RAM** | ~12GB (3 nodes) | ~8GB (single host) |
| **Disk** | ~10GB | ~5GB |
| **Thời gian setup** | ~15 phút (lần đầu build) | ~5 phút |
| **Debug** | kubectl logs | docker compose logs |



# 3. Xử Lý Dữ Liệu với Apache Spark

## 3.1 Tổng quan

Apache Spark 3.5.1 là engine xử lý chính của pipeline, đảm nhận việc:
- Đọc dữ liệu thô (Parquet từ MinIO S3 hoặc Kafka)
- **Enrichment**: Cast kiểu, join zone lookup, tạo derived columns
- **Validation**: 10 rules kiểm tra chất lượng dữ liệu
- **Split**: Tách valid/invalid, ghi vào MinIO S3

Có 2 chế độ xử lý:
1. **Spark Batch** (`spark_local_batch.py`): local[*], một lần cho backfill lịch sử
2. **Spark Streaming** (`spark_stream_taxi_events.py`): Kafka consumer, micro-batch

Ngoài ra còn có:
- `spark_quality_report.py` — Quality report không cần Spark runtime (dùng PyArrow)
- `spark_batch_backfill.py` — Placeholder (legacy)
- `kafka_stream_processor.py` — Python-only processor (không Spark, dùng Kafka-Python + Pandas)

---

## 3.2 Spark Batch Processor

**File**: `jobs/spark_local_batch.py`

### 3.2.1 Usage

### Kubernetes (Skaffold/Airflow) ⭐

Airflow DAG `nyc_e2e_pipeline` tự động submit Spark batch qua `KubernetesPodOperator`:

```python
# Airflow DAG task — tự động chạy mỗi tháng
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    cmds=["/opt/spark/bin/spark-submit"],
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,...",
        "--conf", "spark.jars.ivy=/opt/project/.ivy2",
        "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
        "/opt/project/jobs/spark_local_batch.py",
        "--input", "s3a://nyc-raw/yellow_taxi/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/yellow_tripdata_{{ logical_date.strftime('%Y') }}-{{ logical_date.strftime('%m') }}.parquet",
        "--lookup", "s3a://nyc-lookup/taxi_zone_lookup.csv",
    ],
    env_vars=[
        {"MINIO_ENDPOINT": "http://svc-minio:9000"},
        {"MINIO_ACCESS_KEY": "minio"},
        {"MINIO_SECRET_KEY": "minio123"},
    ],
)
```

**Đặc điểm trong Airflow:**
- Dùng `logical_date` (Jinja template) để lấy year/month từ schedule
- Ivy cache tại `/opt/project/.ivy2/` (shared PVC) — tránh re-download mỗi lần
- S3A packages qua `--packages` CLI
- Mount PVC project-files vào `/opt/project`

### Docker Compose (Legacy)

```bash
make spark-batch
# hoặc:
docker run --rm \
  --network nyc_new_default \
  -v $(pwd):/opt/project -w /opt/project \
  --entrypoint /opt/spark/bin/spark-submit \
  apache/spark:3.5.1 \
  --master local[*] \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,..." \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/project/jobs/spark_local_batch.py \
  --input "s3a://nyc-raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet" \
  --lookup "s3a://nyc-lookup/taxi_zone_lookup.csv"
```

### 3.2.2 Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--input` | required | Path to input Parquet file (S3 hoặc local) |
| `--lookup` | required | Path to taxi_zone_lookup.csv |
| `--silver` | `s3a://nyc-silver/trips` | Output path cho valid trips |
| `--quarantine` | `s3a://nyc-quarantine/invalid_trips` | Output path cho invalid trips |

### 3.2.3 Spark Config (từ SparkSession)

```python
spark = SparkSession.builder \
    .appName("LocalBatchEnriched") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)      # MinIO S3 endpoint
    .config("spark.hadoop.fs.s3a.access.key", access_key)   # minio
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)   # minio123
    .config("spark.hadoop.fs.s3a.path.style.access", "true") # MinIO cần path-style
    .getOrCreate()
```

**K8s mode (Airflow) bổ sung:**
```bash
--conf spark.jars.ivy=/opt/project/.ivy2
--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2
--conf spark.scheduler.mode=FAIR
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
```

### 3.2.4 Luồng xử lý chi tiết

#### Bước 1: Đọc dữ liệu
```python
raw = spark.read.parquet(input_path)           # Raw Parquet
zones_raw = spark.read.option("header", "true").csv(lookup_path)  # CSV zone lookup
```

#### Bước 2: Enrichment
```python
enriched = raw.select(
    col("VendorID").cast("int").alias("vendor_id"),
    to_timestamp("tpep_pickup_datetime").alias("pickup_ts"),
    to_timestamp("tpep_dropoff_datetime").alias("dropoff_ts"),
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    col("PULocationID").cast("int").alias("pickup_location_id"),
    col("DOLocationID").cast("int").alias("dropoff_location_id"),
    col("payment_type").cast("int"),
    col("fare_amount").cast("double"),
    col("total_amount").cast("double"),
    # ... các trường khác
)
```

**Derived columns được thêm vào:**
- `trip_id`: xxhash64 của `pickup_ts|pickup_location_id|dropoff_location_id`
- `source_file`: tên file Parquet gốc
- `event_ts`, `ingestion_ts`: thời gian xử lý
- `pickup_date`, `pickup_hour`, `pickup_year`, `pickup_month`: partition columns
- `pickup_borough/zone/service_zone` và `dropoff_*`: từ zone lookup join

#### Bước 3: Zone Lookup Join
```python
# Chuẩn bị 2 lookup tables
pickup_zones = zones.select(
    col("location_id").alias("pickup_location_id"),
    col("borough").alias("pickup_borough"),
    col("zone").alias("pickup_zone"),
    col("service_zone").alias("pickup_service_zone"),
)
dropoff_zones = zones.select(
    col("location_id").alias("dropoff_location_id"),
    col("borough").alias("dropoff_borough"),
    col("zone").alias("dropoff_zone"),
    col("service_zone").alias("dropoff_service_zone"),
)

# Left join
enriched = enriched.join(pickup_zones, on="pickup_location_id", how="left")
enriched = enriched.join(dropoff_zones, on="dropoff_location_id", how="left")
```

#### Bước 4: Validation (10 rules)
```python
error_array = array(
    when(col("pickup_ts").isNull(), lit("pickup_datetime_null_or_invalid")),
    when(col("dropoff_ts").isNull(), lit("dropoff_datetime_null_or_invalid")),
    when(col("dropoff_ts") <= col("pickup_ts"), lit("invalid_trip_duration")),
    when(col("trip_distance") <= 0, lit("non_positive_trip_distance")),
    when(col("fare_amount") < 0, lit("negative_fare_amount")),
    when(col("total_amount") < col("fare_amount"), lit("total_amount_less_than_fare")),
    when(...passenger_count out of 1-6..., lit("invalid_passenger_count")),
    when(...payment_type not 1-6..., lit("payment_type_out_of_range")),
    when(...pickup_borough null..., lit("unknown_pickup_location")),
    when(...dropoff_borough null..., lit("unknown_dropoff_location")),
)

validated = enriched
    .withColumn("validation_error_candidates", error_array)
    .withColumn("validation_errors",
                expr("filter(validation_error_candidates, x -> x is not null)"))
    .withColumn("is_valid", size(col("validation_errors")) == lit(0))
    .withColumn("quarantine_ts", current_timestamp())
```

#### Bước 5: Split và ghi
```python
valid = validated.filter(col("is_valid"))
invalid = validated.filter(~col("is_valid"))

# Valid: partitioned by pickup_year, pickup_month
valid.select(silver_columns) \
    .write.partitionBy("pickup_year", "pickup_month") \
    .mode("append") \
    .parquet(silver_path)

# Invalid: non-partitioned + kèm validation_errors
invalid.select(silver_columns + ["validation_errors", "quarantine_ts"]) \
    .write.mode("append") \
    .parquet(quarantine_path)
```

> **Lưu ý**: Luôn dùng `mode("append")` — không dùng `overwrite` 
> để tránh mất dữ liệu do `partitionOverwriteMode=dynamic`.

### 3.2.5 Columns đầu ra (Silver)

| Column | Type | Description |
|--------|------|-------------|
| `trip_id` | BIGINT | xxhash64(pickup_ts, pickup_loc, dropoff_loc) |
| `source_file` | VARCHAR | Tên file Parquet gốc |
| `vendor_id` | INTEGER | 1=Creative Mobile, 2=VeriFone |
| `pickup_ts` | TIMESTAMP | Thời gian đón khách |
| `dropoff_ts` | TIMESTAMP | Thời gian trả khách |
| `passenger_count` | INTEGER | 1-6 |
| `trip_distance` | DOUBLE | Dặm |
| `rate_code_id` | INTEGER | 1=Standard, 2=JFK... |
| `pickup_location_id` | INTEGER | Zone ID (1-265) |
| `dropoff_location_id` | INTEGER | Zone ID |
| `payment_type` | INTEGER | 1=Credit, 2=Cash... |
| `fare_amount` | DOUBLE | Giá cước |
| `extra` | DOUBLE | Phụ phí |
| `mta_tax` | DOUBLE | Thuế MTA ($0.50) |
| `tip_amount` | DOUBLE | Tiền tip |
| `tolls_amount` | DOUBLE | Phí cầu đường |
| `improvement_surcharge` | DOUBLE | Phụ phí ($0.30) |
| `total_amount` | DOUBLE | Tổng tiền |
| `pickup_borough/zone/service_zone` | VARCHAR | Thông tin vùng đón |
| `dropoff_borough/zone/service_zone` | VARCHAR | Thông tin vùng trả |
| `pickup_year/month/date/hour` | INT/DATE | Partition + temporal columns |
| `event_ts`, `ingestion_ts` | TIMESTAMP | Metadata timestamps |

---

## 3.3 Spark Streaming Processor

**File**: `jobs/spark_stream_taxi_events.py`

### 3.3.1 Usage

### Kubernetes (Skaffold/Airflow) ⭐

Airflow DAG `nyc_e2e_pipeline` tự động submit Spark streaming với `--trigger-available-now`:

```python
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,...",
        "/opt/project/jobs/spark_stream_taxi_events.py",
        "--bootstrap-server", "svc-kafka:9092",
        "--topic", "taxi.trip.events",
        "--trigger-available-now",
        "--checkpoint-path", "s3a://nyc-silver/checkpoints/spark_stream_taxi_events/...",
    ],
    env_vars=[{"MINIO_ENDPOINT": "http://svc-minio:9000"}, ...],
)
```

**Lưu ý K8s:**
- Kafka bootstrap: `svc-kafka:9092` (⚠️ prefix `svc-`)
- Checkpoint trên S3: `s3a://nyc-silver/checkpoints/...`
- Cần `spark-sql-kafka-0-10` package

### Docker Compose (Legacy)

```bash
make spark-streaming
# hoặc:
TOPIC=taxi.trip.events bash scripts/start_streaming_job_docker.sh
```

### 3.3.2 Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--bootstrap-server` | localhost:29092 | Kafka bootstrap servers |
| `--topic` | taxi.trip.events | Kafka input topic |
| `--lookup-path` | s3a://nyc-lookup/taxi_zone_lookup.csv | Zone lookup |
| `--silver-path` | s3a://nyc-silver/trips | Output silver |
| `--quarantine-path` | s3a://nyc-quarantine/invalid_trips | Output quarantine |
| `--checkpoint-path` | data/checkpoints/... | Streaming checkpoint |
| `--trigger-available-now` | false | Micro-batch one-shot mode |

### 3.3.3 Kafka Event Schema

```python
EVENT_SCHEMA = StructType([
    StructField("event_id",           StringType(), True),
    StructField("event_timestamp",    StringType(), True),
    StructField("source_file",        StringType(), True),
    StructField("vendor_id",          IntegerType(), True),
    StructField("pickup_datetime",    StringType(), True),
    StructField("dropoff_datetime",   StringType(), True),
    StructField("passenger_count",    IntegerType(), True),
    StructField("trip_distance",      DoubleType(), True),
    StructField("rate_code_id",       IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id",IntegerType(), True),
    StructField("payment_type",       IntegerType(), True),
    StructField("fare_amount",        DoubleType(), True),
    StructField("extra",              DoubleType(), True),
    StructField("mta_tax",            DoubleType(), True),
    StructField("tip_amount",         DoubleType(), True),
    StructField("tolls_amount",       DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount",       DoubleType(), True),
])
```

### 3.3.4 Luồng xử lý

```python
# 1. Read stream từ Kafka
raw = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", args.bootstrap_server)
    .option("subscribe", args.topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

# 2. Parse JSON value
parsed = raw.select(
    col("value").cast("string").alias("raw_value"),
    from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    col("topic"), col("partition"), col("offset"), col("timestamp"),
)

# 3. Enrich (giống batch)
# 4. Validate (giống batch)
# 5. foreachBatch micro-batch
validated.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", args.checkpoint_path)
    .trigger(availableNow=True)  # one-shot mode
    .start()
```

### 3.3.5 foreachBatch Writer

```python
def write_batch(batch_df, batch_id):
    batch_df.persist()
    
    valid_df = batch_df.filter(col("is_valid") == True)
    invalid_df = batch_df.filter(col("is_valid") == False)
    
    if not valid_df.rdd.isEmpty():
        valid_df.select(silver_columns) \
            .write.mode("append") \
            .partitionBy("pickup_year", "pickup_month") \
            .parquet(args.silver_path)
    
    if not invalid_df.rdd.isEmpty():
        invalid_df.write.mode("append") \
            .parquet(args.quarantine_path)
    
    batch_df.unpersist()
```

---

## 3.4 Python-Only Kafka Processor

**File**: `jobs/kafka_stream_processor.py`

Alternative processor sử dụng **Kafka-Python + Pandas + PyArrow** (không cần Spark).

### 3.4.1 Đặc điểm

- **Không cần Spark runtime** — lightweight, chạy trên Python thuần
- **Poll-based loop**: consumer.poll() với max-empty-polls tự động thoát
- **Write**: PyArrow `write_to_dataset()` cho valid, `write_table()` cho invalid
- **Tốc độ**: Chậm hơn Spark, phù hợp development/testing

### 3.4.2 Usage
```bash
python3 jobs/kafka_stream_processor.py \
  --bootstrap-server localhost:29092 \
  --topic taxi.trip.events \
  --lookup-path data/lookup/taxi_zone_lookup.csv \
  --silver-path data/silver/trips \
  --quarantine-path data/quarantine/invalid_trips
```

---

## 3.5 Quality Report

**File**: `jobs/spark_quality_report.py`

Sinh báo cáo chất lượng dữ liệu bằng PyArrow (không cần Spark).

```bash
make quality-report
# hoặc
python3 jobs/spark_quality_report.py \
  --silver-path data/silver/trips \
  --quarantine-path data/quarantine/invalid_trips \
  --output reports/data_quality_report.md
```

**Output example:**
```markdown
# Data Quality Report

Generated at: 2026-06-12T10:30:00.000000+00:00

- Total records processed: **9,554,778**
- Valid records: **8,480,408**
- Invalid records: **1,074,370**
- Invalid percentage: **11.24%**
```

---

## 3.6 Lưu ý quan trọng

### S3A Package
```bash
# BẮT BUỘC: dùng --packages trên spark-submit CLI
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

# KHÔNG dùng spark.jars.packages trong SparkSession config
```

### Ivy Cache
```bash
# Chia sẻ Ivy cache trên PVC để tránh re-download mỗi lần
--conf spark.jars.ivy=/opt/project/.ivy2/
# Permissions
chmod -R 777 /opt/project/.ivy2/
```

### MinIO S3 Commit Fix
```python
# BẮT BUỘC: MinIO không hỗ trợ atomic S3 rename
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
```

### Output Mode
```python
# Luôn dùng mode("append") — không overwrite
# partitionOverwriteMode=dynamic không hoạt động đúng với MinIO
```

### Partition Columns
```python
# Valid trips: partitioned by pickup_year, pickup_month
# Columns phải là LAST trong select
```

### Streaming Checkpoint
```python
# Checkpoint trên S3 cho K8s mode
--checkpoint-path "s3a://nyc-silver/checkpoints/spark_stream_taxi_events/..."
# Checkpoint local cho Docker mode
--checkpoint-path "data/checkpoints/spark_stream_taxi_events"
```



# 4. dbt Models và Data Transformation

## 4.1 Tổng quan

dbt-trino là tầng biến đổi dữ liệu, chuyển đổi dữ liệu silver (Parquet trên MinIO) 
thành các view phân tích có cấu trúc. Pipeline có **15 models**, **9 tests**, 
kỳ vọng **24/24 PASS**.

### Cấu trúc layers

```mermaid
flowchart RL
    subgraph GOLD["🥇 Gold Layer (4 models)"]
        G1["gold_fact_trips"]
        G2["gold_dim_zone"]
        G3["gold_mart_revenue_by_day"]
        G4["gold_mart_revenue_by_zone"]
    end
    subgraph MART["📊 Mart Layer (8 models)"]
        M1["fact_trips"]
        M2["fact_invalid_trips"]
        M3["dim_zone"]
        M4["mart_hourly_summary"]
        M5["mart_revenue_by_day"]
        M6["mart_revenue_by_zone"]
        M7["mart_payment_type_summary"]
        M8["mart_trips_by_hour"]
    end
    subgraph STAGING["📋 Staging Layer (3 models)"]
        S1["stg_trips"]
        S2["stg_zones"]
        S3["stg_invalid_trips"]
    end
    subgraph TRINO_HIVE["🗄️ Trino Hive External Tables"]
        H1["hive.nyc.trips"]
        H2["hive.nyc.invalid_trips"]
        H3["hive.nyc.taxi_zone_lookup"]
    end
    subgraph MINIO_S3["💾 MinIO S3 Silver Data"]
        MIO1["s3://nyc-silver/trips/"]
        MIO2["s3://nyc-quarantine/"]
        MIO3["s3://nyc-lookup/taxi_zone_lookup.csv"]
    end

    MINIO_S3 --> TRINO_HIVE --> STAGING --> MART --> GOLD
```

### Configuration

**File**: `dbt/dbt_project.yml`
```yaml
name: nyc_taxi
version: "1.0.0"
profile: nyc_taxi
config-version: 2
model-paths: ["models"]
test-paths: ["tests"]

models:
  nyc_taxi:
    staging:
      +materialized: view
    marts:
      +materialized: view
    gold:
      +materialized: view
```

> ⚠️ **Tất cả models là `view`** — Hive file-based HMS (Hive Metastore) 
> không hỗ trợ `RENAME TABLE` mà dbt cần cho table swaps.

**File**: `dbt/profiles.yml`
```yaml
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: trino
      host: "{{ env_var('TRINO_HOST', 'svc-trino') }}"
      port: "{{ env_var('TRINO_PORT', '8080') | int }}"
      user: dbt
      database: hive
      schema: mart
      threads: 4
```

---

## 4.2 Staging Layer (3 models)

Làm sạch và cast kiểu dữ liệu từ Hive external tables.

### stg_trips

**File**: `dbt/models/staging/stg_trips.sql`

```sql
{{ config(materialized='view') }}

with src as (
  select
    cast(trip_id as bigint)            as trip_id,
    cast(source_file as varchar)       as source_file,
    cast(vendor_id as integer)         as vendor_id,
    cast(pickup_ts as timestamp)       as pickup_ts,
    cast(dropoff_ts as timestamp)      as dropoff_ts,
    cast(passenger_count as integer)   as passenger_count,
    cast(trip_distance as double)      as trip_distance,
    -- ... các trường khác
    cast(pickup_year as integer)       as pickup_year,
    cast(pickup_month as integer)      as pickup_month
  from hive.nyc.trips  -- External table từ Trino Hive catalog
)
select * from src
```

**Vai trò**: Cast tất cả columns từ Parquet types → SQL types qua Hive.
**Nguồn**: `hive.nyc.trips` (external table pointing to `s3://nyc-silver/trips/`)

### stg_zones

**File**: `dbt/models/staging/stg_zones.sql`

```sql
select
  cast(location_id as integer) as location_id,
  borough,
  zone,
  service_zone
from hive.nyc.taxi_zone_lookup
```

**Nguồn**: `hive.nyc.taxi_zone_lookup` (CSV external table)

### stg_invalid_trips

**File**: `dbt/models/staging/stg_invalid_trips.sql`

```sql
with src as (
  select
    cast(vendor_id as integer)      as vendor_id,
    cast(pickup_ts as timestamp)    as pickup_ts,
    cast(dropoff_ts as timestamp)   as dropoff_ts,
    cast(fare_amount as double)     as fare_amount,
    cast(total_amount as double)    as total_amount,
    validation_errors,                        -- ARRAY<VARCHAR>
    cast(quarantine_ts as timestamp) as quarantine_ts,
    cast(pickup_year as integer)    as pickup_year,
    cast(pickup_month as integer)   as pickup_month
  from hive.nyc.invalid_trips
)
select * from src
```

---

## 4.3 Mart Layer (8 models)

Các models phân tích với derived fields và aggregations.

### fact_trips

**File**: `dbt/models/marts/fact_trips.sql`

Model chính — fact table với mọi chuyến đi hợp lệ.

```sql
select
  pickup_ts,
  dropoff_ts,
  date_trunc('hour', pickup_ts)         as pickup_hour_ts,
  date(pickup_ts)                       as pickup_date,
  hour(pickup_ts)                       as pickup_hour,
  day_of_week(pickup_ts)                as pickup_dow,
  vendor_id,
  passenger_count,
  trip_distance,
  rate_code_id,
  payment_type,
  fare_amount, extra, mta_tax, tip_amount,
  tolls_amount, improvement_surcharge, total_amount,
  -- Derived fields
  case when total_amount > 0 
    then tip_amount / total_amount end  as tip_rate,
  date_diff('second', pickup_ts, dropoff_ts) as trip_duration_sec,
  -- Zone info
  pickup_location_id, dropoff_location_id,
  pickup_zone, dropoff_zone,
  pickup_borough, dropoff_borough,
  pickup_service_zone, dropoff_service_zone,
  pickup_year, pickup_month
from {{ ref('stg_trips') }}
```

**Derived fields:**
- `tip_rate`: tip_amount / total_amount
- `trip_duration_sec`: date_diff second giữa dropoff và pickup
- `pickup_hour_ts`: Trunc ngày xuống hour
- `pickup_dow`: Day of week (1=Sunday...7=Saturday)

### dim_zone

**File**: `dbt/models/marts/dim_zone.sql`

Dimension zone — union tất cả zones từ pickup và dropoff.

```sql
with zones as (
  select pickup_zone as zone, pickup_borough as borough, 
         pickup_service_zone as service_zone 
  from {{ ref('stg_trips') }}
  union
  select dropoff_zone, dropoff_borough, dropoff_service_zone
  from {{ ref('stg_trips') }}
)
select
  row_number() over (order by zone) as zone_sk,
  zone,
  any_value(borough)       as borough,
  any_value(service_zone)  as service_zone
from zones
where zone is not null
group by zone
```

### fact_invalid_trips

**File**: `dbt/models/marts/fact_invalid_trips.sql`

Invalid trips fact — explode ARRAY validation_errors thành từng error reason.

```sql
select
  quarantine_ts,
  cast(pickup_year as integer)   as pickup_year,
  cast(pickup_month as integer)  as pickup_month,
  err as validation_error,
  count(*) as error_count
from {{ ref('stg_invalid_trips') }}
cross join unnest(validation_errors) as t(err)
group by 1, 2, 3, 4
```

### mart_hourly_summary

**File**: `dbt/models/marts/mart_hourly_summary.sql`

```sql
select
  pickup_date,
  pickup_hour,
  pickup_borough,
  count(*)          as trip_count,
  avg(fare_amount)  as avg_fare,
  avg(total_amount) as avg_total,
  avg(trip_distance) as avg_distance,
  sum(total_amount) as gross_revenue
from {{ ref('fact_trips') }}
group by 1, 2, 3
```

### mart_revenue_by_day

**File**: `dbt/models/marts/mart_revenue_by_day.sql`

```sql
select
  pickup_date,
  count(*)           as trip_count,
  sum(fare_amount)   as total_fare,
  sum(extra)         as total_extra,
  sum(mta_tax)       as total_mta_tax,
  sum(tip_amount)    as total_tip,
  sum(tolls_amount)  as total_tolls,
  sum(improvement_surcharge) as total_improvement_surcharge,
  sum(total_amount)  as gross_revenue,
  avg(fare_amount)   as avg_fare,
  avg(total_amount)  as avg_total,
  avg(tip_amount)    as avg_tip,
  avg(trip_distance) as avg_distance
from {{ ref('fact_trips') }}
group by 1
order by 1
```

### mart_revenue_by_zone

**File**: `dbt/models/marts/mart_revenue_by_zone.sql`

```sql
select
  pickup_borough, pickup_zone,
  dropoff_borough, dropoff_zone,
  count(*)          as trip_count,
  sum(total_amount) as gross_revenue,
  avg(total_amount) as avg_revenue_per_trip,
  sum(fare_amount)  as total_fare,
  sum(tip_amount)   as total_tip,
  avg(trip_distance) as avg_distance
from {{ ref('fact_trips') }}
group by 1, 2, 3, 4
order by gross_revenue desc
```

### mart_payment_type_summary

**File**: `dbt/models/marts/mart_payment_type_summary.sql`

```sql
select
  payment_type,
  case payment_type
    when 1 then 'Credit card'
    when 2 then 'Cash'
    when 3 then 'No charge'
    when 4 then 'Dispute'
    when 5 then 'Unknown'
    when 6 then 'Voided'
    else 'Other'
  end as payment_type_name,
  count(*)           as trip_count,
  sum(total_amount)  as gross_revenue,
  avg(total_amount)  as avg_revenue_per_trip,
  sum(tip_amount)    as total_tip,
  avg(tip_amount)    as avg_tip,
  sum(fare_amount)   as total_fare,
  avg(trip_distance) as avg_distance
from {{ ref('fact_trips') }}
group by 1
order by gross_revenue desc
```

### mart_trips_by_hour

**File**: `dbt/models/marts/mart_trips_by_hour.sql`

```sql
select
  pickup_hour,
  pickup_dow,
  count(*)              as trip_count,
  sum(total_amount)     as gross_revenue,
  avg(total_amount)     as avg_revenue_per_trip,
  avg(trip_distance)    as avg_distance,
  avg(trip_duration_sec) as avg_duration_sec
from {{ ref('fact_trips') }}
group by 1, 2
order by 1, 2
```

---

## 4.4 Gold Layer (4 models)

Gold layer là các view tương tự Mart layer nhưng clean hơn, 
dùng cho export và analytics cuối cùng. Các models:

| Model | Description |
|-------|-------------|
| `gold_fact_trips` | fact_trips + trip_id và source_file |
| `gold_dim_zone` | dim_zone với location_id |
| `gold_mart_revenue_by_day` | Giống mart_revenue_by_day nhưng ref gold_fact_trips |
| `gold_mart_revenue_by_zone` | Giống mart_revenue_by_zone nhưng ref gold_fact_trips |

Ví dụ **gold_fact_trips.sql**:
```sql
select
  trip_id,
  source_file,
  vendor_id,
  pickup_ts, dropoff_ts,
  -- ... tất cả columns như fact_trips + trip_id và source_file
  pickup_year, pickup_month
from {{ ref('stg_trips') }}
```

---

## 4.5 Tests (9 tests)

### YAML Generic Tests

**File**: `dbt/tests/stg_trips_tests.yml`
```yaml
version: 2
models:
  - name: stg_trips
    columns:
      - name: pickup_ts
        tests: [not_null]
      - name: dropoff_ts
        tests: [not_null]
      - name: trip_distance
        tests: [not_null]
      - name: total_amount
        tests: [not_null]
      - name: payment_type
        tests: [not_null]
```

**File**: `dbt/tests/fact_trips_tests.yml`
```yaml
version: 2
models:
  - name: fact_trips
    columns:
      - name: pickup_ts
        tests: [not_null]
      - name: total_amount
        tests: [not_null]
```

**File**: `dbt/tests/fact_invalid_trips_tests.yml` và `stg_trips_tests.yml`:
- not_null trên các key columns
- accepted_values cho payment_type (1-6)

### Singular SQL Test

**File**: `dbt/tests/payment_type_range.sql`
```sql
-- Đảm bảo payment_type trong khoảng 1-6
select payment_type, count(*) as cnt
from {{ ref('stg_trips') }}
where payment_type < 1 or payment_type > 6
group by payment_type
```

**Tổng hợp tests:**
- 5 `not_null` tests (stg_trips)
- 2 `not_null` tests (fact_trips)
- 1 `accepted_values` test (payment_type range)
- 1 singular test (payment_type_range.sql)

Kỳ vọng: **24/24 PASS** (15 models + 9 tests).

---

## 4.6 Dòng chảy dữ liệu qua dbt

```mermaid
flowchart TD
    TRIPS["hive.nyc.trips"]
    ZONES_RAW["hive.nyc.taxi_zone_lookup"]
    
    subgraph STG["Staging"]
        ST["stg_trips"]
        SZ["stg_zones"]
        SI["stg_invalid_trips"]
    end

    subgraph MART["Mart"]
        FT["fact_trips"]
        FI["fact_invalid_trips"]
        DZ["dim_zone"]
        MHS["mart_hourly_summary"]
        MRD["mart_revenue_by_day"]
        MRZ["mart_revenue_by_zone"]
        MPT["mart_payment_type_summary"]
        MTH["mart_trips_by_hour"]
    end

    subgraph GOLD["Gold"]
        GFT["gold_fact_trips"]
        GDZ["gold_dim_zone"]
        GMRD["gold_mart_revenue_by_day"]
        GMRZ["gold_mart_revenue_by_zone"]
    end

    TRIPS --> ST
    ZONES_RAW --> SZ
    TRIPS --> SI
    
    ST --> FT
    ST --> FI
    SZ --> DZ
    ST --> MHS & MRD & MRZ & MPT & MTH
    FI --> MHS & MRD & MRZ & MPT & MTH
    DZ --> MHS & MRD & MRZ & MPT & MTH

    FT --> GFT
    DZ --> GDZ
    GFT --> GMRD & GMRZ
```

---

## 4.7 Commands

### Kubernetes (Airflow DAG) ⭐

Airflow tự động chạy dbt build trong `nyc_e2e_pipeline` và `nyc_analytics_refresh`:

```python
KubernetesPodOperator(
    image="nyc-dbt:k8s",
    cmds=["entrypoint-dbt"],
    env_vars=[
        ("DBT_PROFILES_DIR", "/opt/project/dbt"),
        ("TRINO_HOST", "svc-trino"),
    ],
    volumes=[project_volume],
    volume_mounts=[project_volume_mount],
)
```

**entrypoint-dbt.sh sẽ:**
1. Wait Trino ready (TCP check svc-trino:8080)
2. Sync Hive partitions (trino_sync_partitions.py)
3. `cd /opt/project/dbt && dbt build`

### Docker Compose (Legacy)

```bash
make dbt-build    # Full: dbt build
make dbt-run      # Models only
make dbt-test     # Tests only
```

### Row counts kỳ vọng

| Table | Rows |
|-------|------|
| dim_zone | ~261 |
| fact_trips | ~8-10M |
| mart_hourly_summary | ~11K+ |
| mart_revenue_by_day | ~90-96 |
| mart_revenue_by_zone | ~25K |
| mart_payment_type_summary | 6 |
| mart_trips_by_hour | ~168 (24h × 7 days) |



# 5. Trino Catalog và Hive Metadata

## 5.1 Tổng quan

**Trino 435** đóng vai trò SQL query engine, kết nối MinIO S3 qua Hive connector. 
Nó cung cấp khả năng truy vấn SQL trực tiếp trên dữ liệu Parquet mà không cần Spark.

### Kiến trúc

```mermaid
graph TB
    subgraph TRINO["Trino Coordinator"]
        TRINO_URL["http://svc-trino:8080"]
        HIVE_CONN["Hive Connector (hive.*)<br/>• File-based metastore<br/>• Đọc parquet từ MinIO S3"]
    end

    subgraph MINIO["MinIO S3"]
        S1["s3://nyc-silver/trips/"]
        S2["s3://nyc-quarantine/invalid_trips/"]
        S3["s3://nyc-lookup/taxi_zone_lookup.csv"]
    end

    TRINO_URL --> HIVE_CONN
    HIVE_CONN -->|s3://| MINIO
```

---

## 5.2 Cấu hình Trino

### Hive Catalog Properties

**File**: `docker/trino/etc/catalog/hive.properties`
```properties
connector.name=hive
hive.metastore=file
hive.metastore.catalog.dir=/data/trino-metastore
hive.allow-drop-table=true
hive.recursive-directories=true
hive.orc.use-column-names=true
hive.parquet.use-column-names=true
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minio
hive.s3.aws-secret-key=minio123
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
hive.non-managed-table-creates-enabled=true
hive.non-managed-table-writes-enabled=true
```

**Cấu hình chính:**
| Property | Value | Ý nghĩa |
|----------|-------|---------|
| `hive.metastore` | `file` | File-based metastore (không cần Hive Metastore service riêng) |
| `hive.metastore.catalog.dir` | `/data/trino-metastore` | Thư mục lưu metadata |
| `hive.recursive-directories` | `true` | Đọc sub-directories |
| `hive.s3.*` | MinIO config | Kết nối S3-compatible storage |
| `hive.non-managed-table-creates-enabled` | `true` | Cho phép tạo external tables |
| `hive.non-managed-table-writes-enabled` | `true` | Cho phép ghi external tables (dùng cho gold export) |

### Config Properties

**File**: `docker/trino/etc/config.properties`
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
```

### JVM Config

**File**: `docker/trino/etc/jvm.config`
```
-server
-Xmx2G
```

---

## 5.3 Đăng ký Tables (Trino Bootstrap)

**Script**: `scripts/trino_register.py`

Quy trình đăng ký idempotent:
1. Chờ Trino coordinator ready (TCP connect, timeout 120s)
2. Tạo schema `hive.nyc` (CREATE SCHEMA IF NOT EXISTS)
3. Tạo hoặc thay thế (DROP + CREATE) các external tables

### Trips Table (Partitioned)
```sql
CREATE TABLE hive.nyc.trips (
  trip_id BIGINT,
  source_file VARCHAR,
  vendor_id INTEGER,
  pickup_ts TIMESTAMP,
  -- ... tất cả columns
  pickup_year INTEGER,
  pickup_month INTEGER
)
WITH (
  external_location = 's3://nyc-silver/trips',
  format = 'PARQUET',
  partitioned_by = ARRAY['pickup_year','pickup_month']
)
```

### Invalid Trips Table (Non-partitioned)
```sql
CREATE TABLE hive.nyc.invalid_trips (
  validation_errors ARRAY(VARCHAR),
  quarantine_ts TIMESTAMP,
  -- ... các trường giống trips
)
WITH (
  external_location = 's3://nyc-quarantine/invalid_trips',
  format = 'PARQUET'
)
```

### Taxi Zone Lookup (CSV External)
```sql
CREATE TABLE hive.nyc.taxi_zone_lookup (
  location_id VARCHAR,
  borough VARCHAR,
  zone VARCHAR,
  service_zone VARCHAR
)
WITH (
  external_location = 's3://nyc-lookup/',
  format = 'CSV',
  csv_separator = ',',
  csv_escape = '\\',
  csv_quote = '"',
  skip_header_line_count = 1
)
```

### Sync Partitions

Sau khi tạo tables, script sync partitions:
```sql
CALL hive.system.sync_partition_metadata(
  schema_name => 'nyc',
  table_name => 'trips',
  mode => 'FULL'
)
```

### Smoke Test

Cuối cùng, đếm số dòng:
```sql
SELECT COUNT(*) FROM hive.nyc.trips;
SELECT COUNT(*) FROM hive.nyc.invalid_trips;
SELECT COUNT(*) FROM hive.nyc.taxi_zone_lookup;
```

---

## 5.4 Gold Export Script

**Script**: `scripts/export_gold_to_minio.py`

Script này export datasets từ Trino sang MinIO S3 dưới dạng Parquet 
thông qua CTAS (CREATE TABLE AS SELECT).

### 5.4.1 Luồng xử lý

1. Kết nối Trino, tạo schema `hive.nyc_gold`
2. Với mỗi dataset:
   - Đếm số dòng (SELECT COUNT(*))
   - DROP TABLE IF EXISTS
   - Clean S3 path (xoá objects cũ)
   - CTAS: `CREATE TABLE hive.nyc_gold.{name} WITH (external_location = 's3://nyc-gold/{name}/', format = 'PARQUET') AS {sql}`

### 5.4.2 Các datasets được export

**Nhóm Fact Tables:**
| Dataset | Partitioned | Description |
|---------|-------------|-------------|
| `fact_trips` | Yes | Chuyến đi với derived fields (tip_rate, trip_duration_sec) |
| `fact_trips_enriched` | Yes | fact_trips + inferred_purpose, is_airport_trip, trip_time_category |
| `fact_trips_daily` | No | Tổng hợp theo ngày |
| `fact_trips_hourly` | No | Tổng hợp theo giờ |
| `fact_trips_hourly_zone` | No | Tổng hợp theo giờ + zone |
| `fact_trips_borough` | No | Tổng hợp theo borough |

**Nhóm Dimension Tables:**
| Dataset | Description |
|---------|-------------|
| `dim_zone` | Zone lookup với location_id |
| `dim_zone_grouped` | Zone + trip_count, volume tier (High/Medium/Low) |
| `dim_date` | Date dimension (date, year, month, day, is_weekend...) |
| `dim_vendor` | 2 vendors mapping |
| `dim_payment_type` | 6 payment types |
| `dim_rate_code` | 6 rate codes |

**Nhóm KPI & Business Metrics:**
| Dataset | Description |
|---------|-------------|
| `kpi_daily_overview` | Daily KPI: trips, revenue, avg_fare, tip%, utilization |
| `kpi_weekly_trends` | Weekly: trips, revenue, growth% (WoW) |
| `kpi_monthly_summary` | Monthly: trips, revenue, avg_trip_per_day, MoM growth |
| `kpi_borough_comparison` | Borough: revenue, market_share%, avg metrics |
| `kpi_zone_performance` | Zone-level: pickups, dropoffs, net_flow, airport trips |
| `kpi_zone_net_flow` | Zone net flow: imbalance_score, primary source/dest |
| `kpi_payment_trends` | Payment type: trip_count, revenue, avg_tip% |
| `kpi_vendor_performance` | Vendor: trips, revenue, market_share% |

**Nhóm Route & Operational Analysis:**
| Dataset | Description |
|---------|-------------|
| `route_top_pickup_zones` | Top 20 pickup zones |
| `route_top_dropoff_zones` | Top 20 dropoff zones |
| `route_popular_routes` | Top 50 routes (pickup→dropoff) |
| `route_airport_analysis` | Airport trips (EWR, JFK, LaGuardia) |
| `route_airport_zone_matrix` | Airport → residential zone matrix |
| `route_cross_borough` | Cross-borough trips matrix |
| `od_borough_matrix` | Origin-Destination borough matrix |
| `ops_peak_hours_heatmap` | Hour × DayOfWeek heatmap |
| `ops_trip_distance_distribution` | Distance buckets (0-1, 1-3, 3-5...) |
| `ops_passenger_count_pattern` | Passenger count patterns |
| `ops_utilization_rate` | Tip rate + multi-passenger rate |

**Nhóm Data Quality & Audit:**
| Dataset | Description |
|---------|-------------|
| `dq_validation_summary` | Daily quality: zero_distance, negative_fare... |
| `dq_invalid_by_reason` | Invalid count by reason per day |
| `dq_row_count_trend` | 7-day rolling anomaly detection |
| `dq_batch_metadata` | Export metadata (timestamp, row counts) |

### 5.4.3 Usage

### Kubernetes (Airflow DAG) ⭐

Airflow tự động chạy gold export sau dbt build trong cả 2 DAGs:

```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    cmds=["python3"],
    arguments=["/opt/project/scripts/export_gold_to_minio.py"],
    env_vars=[
        ("TRINO_HOST", "svc-trino"),
        ("TRINO_PORT", "8080"),
    ],
)
```

### Docker Compose (Legacy)
```bash
make gold-export
```

---

## 5.5 Trino Shell

### Kubernetes ⭐
```bash
kubectl exec -n nyc-taxi -it deploy/trino -- trino --user analytics
```

### Docker Compose (Legacy)
```bash
make trino-shell
# hoặc: docker exec -it nyc_trino trino --user analytics
```

**Useful queries:**
```sql
-- List schemas
SHOW SCHEMAS FROM hive;

-- List tables
SHOW TABLES FROM hive.nyc;

-- Describe table
SHOW COLUMNS FROM hive.nyc.trips;

-- Count rows
SELECT COUNT(*) FROM hive.nyc.trips;

-- Show partitions
SELECT * FROM hive.nyc."trips$partitions";

-- List information schema
SELECT table_name, table_type 
FROM hive.information_schema.tables 
WHERE table_schema = 'mart'
ORDER BY table_name;

-- Query dbt view
SELECT * FROM hive.mart.fact_trips LIMIT 10;
```

---

## 5.6 Lưu ý

1. **File-based metastore**: Metadata lưu trong thư mục `/data/trino-metastore`.
   Khi xoá volume này, mất tất cả registered tables.

2. **Hive không hỗ trợ RENAME TABLE**: Dẫn đến hạn chế:
   - dbt models PHẢI là `materialized='view'`
   - Không thể dùng `materialized='table'` (table swap fails)

3. **Partition sync**: Sau khi Spark ghi thêm partitions, cần sync:
   ```sql
   CALL hive.system.sync_partition_metadata(
     schema_name => 'nyc', table_name => 'trips', mode => 'FULL'
   );
   ```

4. **S3 vs S3A**: 
   - Spark dùng `s3a://` (Hadoop S3A connector)
   - Trino dùng `s3://` (Hive S3 connector native)

5. **Query timeout**: Với queries lớn, set session parameter:
   ```sql
   SET SESSION query_max_run_time='120s';
   ```



# 6. Airflow DAGs — Pipeline Orchestration

## 6.1 Tổng quan

Apache Airflow 2.10.5 là công cụ điều phối chính trên Kubernetes. 
Pipeline có **3 DAGs**:

| DAG | Schedule | Mô tả | Tasks |
|-----|----------|-------|-------|
| `nyc_e2e_pipeline` | @monthly | Full E2E: Spark → Trino → dbt → Superset | 7 |
| `nyc_cdc_pipeline` | manual | CDC: Seed Postgres → Debezium → Bridge | 3 |
| `nyc_analytics_refresh` | @weekly | Refresh: dbt → Superset → Analytics | 4 |

### Cấu hình chung

Tất cả DAGs sử dụng **KubernetesPodOperator** (từ `apache-airflow-providers-cncf-kubernetes==8.4.2`):

```python
project_volume = k8s.V1Volume(
    name="project-files",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="project-files-pvc"
    )
)
project_volume_mount = k8s.V1VolumeMount(
    name="project-files",
    mount_path="/opt/project"
)
```

**Service Account**: `airflow-sa` (định nghĩa trong Helm chart RBAC)

---

## 6.2 DAG: nyc_e2e_pipeline

**File**: `airflow/dags/nyc_e2e_pipeline.py`

DAG chính — xử lý end-to-end cho mỗi tháng dữ liệu.

### Luồng thực thi

```
spark_batch ──┐
              ├──→ trino_bootstrap → dbt_build → gold_export → superset_bootstrap → analytics_check
spark_streaming ┘
```

### 7 Tasks chi tiết

#### Task 1: spark_batch
```python
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    cmds=["/opt/spark/bin/spark-submit"],
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,...",
        "--conf", "spark.jars.ivy=/opt/project/.ivy2",
        "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
        "/opt/project/jobs/spark_local_batch.py",
        "--input", "s3a://nyc-raw/yellow_taxi/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/yellow_tripdata_{{ logical_date.strftime('%Y') }}-{{ logical_date.strftime('%m') }}.parquet",
        "--lookup", "s3a://nyc-lookup/taxi_zone_lookup.csv",
    ],
    env_vars=[
        k8s.V1EnvVar(name="MINIO_ENDPOINT", value="http://svc-minio:9000"),
        k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value="minio"),
        k8s.V1EnvVar(name="MINIO_SECRET_KEY", value="minio123"),
    ],
    # ...
)
```

**Đặc điểm**:
- Dùng `logical_date` (Jinja template) để lấy year/month từ schedule
- Chạy `local[*]` — single pod, không cluster mode
- S3A packages via `--packages` CLI (không phải SparkSession config)
- Ivy cache tại `/opt/project/.ivy2/` (shared PVC)

#### Task 2: spark_streaming
```python
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,...",
        "/opt/project/jobs/spark_stream_taxi_events.py",
        "--bootstrap-server", "svc-kafka:9092",
        "--topic", "taxi.trip.events",
        "--trigger-available-now",
    ],
)
```

**Đặc điểm**:
- Kafka bootstrap: `svc-kafka:9092` (⚠️ có prefix `svc-`)
- `--trigger-available-now`: one-shot micro-batch (không phải streaming liên tục)
- Cần `spark-sql-kafka-0-10_2.12:3.5.1` package

#### Task 3: trino_bootstrap
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    cmds=["entrypoint-trino-bootstrap"],
    env_vars=[...],
)
```
Chạy `scripts/trino_register.py` — register Hive external tables.

#### Task 4: dbt_build
```python
KubernetesPodOperator(
    image="nyc-dbt:k8s",
    cmds=["entrypoint-dbt"],
    env_vars=[("DBT_PROFILES_DIR", "/opt/project/dbt"), ("TRINO_HOST", "svc-trino")],
)
```

#### Task 5: gold_export
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    cmds=["python3"],
    arguments=["/opt/project/scripts/export_gold_to_minio.py"],
)
```

#### Task 6: superset_bootstrap
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    arguments=["/opt/project/scripts/superset_bootstrap.py"],
    env_vars=[
        ("SUPERSET_URL", "http://svc-superset:8088"),
        ("TRINO_URI", "trino://analytics@svc-trino:8080/hive/mart"),
    ],
)
```

#### Task 7: analytics_check
```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    arguments=["/opt/project/scripts/run_analytics_questions.py"],
    env_vars=[("TRINO_HOST", "svc-trino"), ("TRINO_PORT", "8080")],
)
```

### Schedule
```python
schedule="@monthly",
start_date=datetime(2024, 1, 1),
end_date=datetime(2024, 3, 31),
catchup=True,
```

Chạy backfill cho tháng 1-3/2024. Với `catchup=True`, Airflow tự động 
tạo DAG runs cho tất cả tháng từ start_date đến end_date, mỗi run 
xử lý 1 tháng với logical_date tương ứng.

---

## 6.3 DAG: nyc_cdc_pipeline

**File**: `airflow/dags/nyc_cdc_pipeline.py`

CDC pipeline — chỉ chạy manual (`schedule=None`).

### Luồng thực thi
```
cdc_seed → cdc_register → cdc_bridge
```

### 3 Tasks

#### Task 1: cdc_seed
```python
KubernetesPodOperator(
    cmds=["entrypoint-cdc-seed"],
    arguments=[
        "--input", "/opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet",
        "--max-rows", "5000",
        "--dsn", "postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi",
    ],
)
```
Đọc Parquet, insert 5000 rows vào Postgres.

#### Task 2: cdc_register
```python
KubernetesPodOperator(
    cmds=["entrypoint-cdc-register"],
    arguments=[
        "--debezium-url", "http://svc-debezium:8083",
        "--postgres-host", "svc-postgres-cdc",
    ],
)
```
Register Debezium Postgres connector qua REST API.

#### Task 3: cdc_bridge
```python
KubernetesPodOperator(
    cmds=["entrypoint-cdc-bridge"],
    arguments=[
        "--bootstrap-server", "svc-kafka:9092",
        "--input-topic", "nyc_cdc.public.trips",
        "--output-topic", "taxi.trip.events",
        "--idle-timeout", "30",
        "--flush-interval", "500",
    ],
)
```
Bridge CDC events → standard format, exits sau 30s idle.

---

## 6.4 DAG: nyc_analytics_refresh

**File**: `airflow/dags/nyc_analytics_refresh.py`

Refresh analytics layer — chạy @weekly.

### Luồng thực thi
```
dbt_build → gold_export → superset_bootstrap → analytics_check
```

### 4 Tasks

Giống với 4 tasks cuối của `nyc_e2e_pipeline`.

```python
schedule="@weekly",
start_date=datetime(2026, 1, 1),
catchup=False,
```

---

## 6.5 K8s Service Account & RBAC

**File**: `charts/nyc-taxi/templates/airflow/rbac.yaml`

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-sa
  namespace: nyc-taxi
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-role
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list", "watch", "create", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-rolebinding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: airflow-role
subjects:
- kind: ServiceAccount
  name: airflow-sa
```

---

## 6.6 Trigger DAGs

### Kubernetes (Primary) ⭐

```bash
# Qua Airflow Web UI (khuyến nghị)
# http://localhost:39085 → admin/admin → unpause DAG → Trigger

# Qua CLI (kubectl exec)
kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_e2e_pipeline

kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_cdc_pipeline

kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_analytics_refresh
```

### Docker Compose (Legacy)
```bash
make airflow-trigger DAG=nyc_e2e_pipeline
```

---

## 6.7 Airflow Entrypoint

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
    airflow users create --username admin --password admin --role Admin
    ;;
esac
```

Airflow dùng **LocalExecutor** với PostgreSQL làm metadata DB.

---

## 6.8 Xử lý lỗi DAG

### Pod execution timeout
```python
DEFAULT_ARGS = {
    "execution_timeout": timedelta(minutes=30),  # e2e pipeline
    # hoặc
    "execution_timeout": timedelta(minutes=15),  # analytics refresh
}
```

### Retry policy
```python
# nyc_cdc_pipeline có retry
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}
# e2e và analytics không retry (chạy lại từ đầu)
```

### Logging
```python
# get_logs=True trong KubernetesPodOperator
# Cho phép xem logs ngay trên Airflow UI
```



# 7. CDC Pipeline — Change Data Capture

## 7.1 Tổng quan

CDC (Change Data Capture) pipeline cung cấp một nguồn sự kiện thay thế, 
cho phép ingest dữ liệu từ **PostgreSQL 16** qua **Debezium 2.5 Kafka Connect**.

### Kiến trúc

```mermaid
graph TB
    PG["Postgres 16<br/>WAL logical<br/>nyc_taxi.trips"]
    DZ["Debezium 2.5<br/>Kafka Connect<br/>pgoutput plugin"]
    IN_TOPIC["Input Topic<br/>nyc_cdc.public.trips<br/>(Debezium envelope)"]
    BRIDGE["cdc_bridge.py<br/>unwrap + transform<br/>ExtractNewRecordState"]
    OUT_TOPIC["Output Topic<br/>taxi.trip.events<br/>(standard format)"]
    SS["Spark Streaming<br/>(cùng logic batch)"]

    PG -->|WAL changes| DZ
    DZ -->|Raw CDC events| IN_TOPIC
    IN_TOPIC --> BRIDGE
    BRIDGE -->|Standard format| OUT_TOPIC
    OUT_TOPIC --> SS
```

---

## 7.2 Postgres Configuration

### Docker Compose
```yaml
nyc_postgres:
  image: postgres:16-alpine
  environment:
    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: postgres
    POSTGRES_DB: nyc_taxi
  command:
    - "postgres"
    - "-c"
    - "wal_level=logical"
    - "-c"
    - "max_replication_slots=4"
    - "-c"
    - "max_wal_senders=4"
```

### Table Schema
```sql
CREATE TABLE IF NOT EXISTS trips (
    trip_id            SERIAL PRIMARY KEY,
    vendor_id          INTEGER,
    pickup_datetime    TIMESTAMP,
    dropoff_datetime   TIMESTAMP,
    passenger_count    INTEGER,
    trip_distance      DOUBLE PRECISION,
    rate_code_id       INTEGER,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type       INTEGER,
    fare_amount        DOUBLE PRECISION,
    extra              DOUBLE PRECISION,
    mta_tax            DOUBLE PRECISION,
    tip_amount         DOUBLE PRECISION,
    tolls_amount       DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount       DOUBLE PRECISION,
    created_at         TIMESTAMP DEFAULT NOW(),
    updated_at         TIMESTAMP DEFAULT NOW()
);

-- BẮT BUỘC cho Debezium capture
ALTER TABLE trips REPLICA IDENTITY FULL;
```

### Postgres Init (Idempotent)

**File**: `docker/entrypoint-init-postgres.sh`

Dùng Python `psycopg2` (không cần `psql`/postgresql-client) để:
1. Chờ Postgres ready (TCP connect retry loop)
2. Tạo bảng `trips` với đầy đủ columns
3. Set `REPLICA IDENTITY FULL`

---

## 7.3 Debezium Configuration

### Docker Compose
```yaml
debezium:
  image: debezium/connect:2.5
  environment:
    BOOTSTRAP_SERVERS: kafka:9092
    GROUP_ID: 1
    CONFIG_STORAGE_TOPIC: debezium_configs
    OFFSET_STORAGE_TOPIC: debezium_offsets
    STATUS_STORAGE_TOPIC: debezium_statuses
```

### Connector Config

**Script**: `scripts/cdc_register_connector.py`

```python
CONNECTOR_CONFIG = {
    "name": "nyc-postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "svc-postgres-cdc",
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
        # Performance tuning
        "poll.interval.ms": "500",
        "max.queue.size": "16384",
        "snapshot.mode": "never",
    },
}
```

**Các config quan trọng:**
| Config | Value | Ý nghĩa |
|--------|-------|---------|
| `plugin.name` | `pgoutput` | Dùng pgoutput plugin (mặc định từ PG10+) |
| `publication.autocreate.mode` | `filtered` | Tự tạo publication cho bảng được include |
| `transforms` | `unwrap` | ExtractNewRecordState — bỏ Debezium envelope |
| `snapshot.mode` | `never` | Không snapshot (dùng cho streaming-only) |
| `poll.interval.ms` | `500` | Tần số poll changes |
| `max.queue.size` | `16384` | Queue size cho high throughput |

---

## 7.4 Seed Postgres từ Parquet

**Script**: `scripts/cdc_seed.py`

Đọc file Parquet NYC TLC và insert vào Postgres:

```bash
cdc-seed --input /opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --max-rows 5000
```

**Luồng xử lý:**
1. Đọc Parquet bằng Pandas
2. Map columns (VendorID → vendor_id, tpep_pickup_datetime → pickup_datetime...)
3. Insert vào Postgres qua SQLAlchemy (chunk-based insert)
4. Mặc định 5000 rows (có thể config qua `--max-rows`)

---

## 7.5 CDC Bridge

**Script**: `scripts/cdc_bridge.py`

Chuyển đổi Debezium unwrapped events → standard NYC Taxi event format.

### Transform Function

```python
def transform(event: dict) -> dict | None:
    """Debezium CDC → standard NYC Taxi event format."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "event_id": f"cdc-{event.get('trip_id', '0')}",
        "event_timestamp": ts,
        "source_file": "cdc:nyc_postgres:nyc_taxi:public.trips",
        "vendor_id": event.get("vendor_id"),
        "pickup_datetime": fmt_micro(event.get("pickup_datetime")),
        "dropoff_datetime": fmt_micro(event.get("dropoff_datetime")),
        # ... các trường khác
    }
```

### Performance

| Mode | Throughput | Cơ chế |
|------|-----------|--------|
| **Async** (default) | ~300-500 ev/s | `producer.send()` + periodic flush every 500 events |
| **Sync** (`--sync`) | ~9 ev/s | `producer.send().get()` per event (~50x slower) |

**Config tối ưu:**
```bash
cdc-bridge \
  --bootstrap-server svc-kafka:9092 \
  --input-topic nyc_cdc.public.trips \
  --output-topic taxi.trip.events \
  --flush-interval 500 \
  --linger-ms 100 \
  --idle-timeout 30
```

### Idle Timeout
Bridge tự động thoát sau N giây không có messages mới:
```python
if args.idle_timeout > 0 and idle >= args.idle_timeout:
    break
```
Mặc định không có timeout (cho streaming liên tục). 
Trong DAG, set `--idle-timeout 30` để job tự kết thúc.

---

## 7.6 CDC Pipeline Flow (đầy đủ)

### Kubernetes (Airflow DAG) ⭐

CDC pipeline chạy qua DAG `nyc_cdc_pipeline` — trigger thủ công:

```bash
kubectl exec -n nyc-taxi deploy/airflow-scheduler -- \
  airflow dags trigger nyc_cdc_pipeline
```

**3 tasks trong DAG:**
1. `cdc_seed` (KubernetesPodOperator, image: nyc-pipeline-tools:k8s) — seed 5000 rows
2. `cdc_register` (KubernetesPodOperator) — register Debezium connector
3. `cdc_bridge` (KubernetesPodOperator) — bridge events với idle-timeout=30s

### Docker Compose (Legacy)

```bash
# Từng bước thủ công
make cdc-seed
make cdc-register
make cdc-bridge
make cdc-verify
```

### Benchmark output (K8s mode)
```
[cdc-bridge] === BENCHMARK ===
[cdc-bridge]   events:       5000
[cdc-bridge]   total time:   11.24s
[cdc-bridge]   throughput:   445 ev/s
[cdc-bridge]   flush calls:  10
[cdc-bridge]   avg latency:  2.2 ms/ev
```

---

## 7.7 Verify CDC

### Kubernetes ⭐
```bash
make k8s-verify-cdc
# hoặc:
kubectl exec -n nyc-taxi statefulset/postgres-cdc -- \
  psql -U postgres -d nyc_taxi -c "SELECT count(*) FROM trips;"

# Kiểm tra Debezium connector
kubectl exec -n nyc-taxi deploy/debezium -- \
  curl -sf http://localhost:8083/connectors/nyc-postgres-connector/status
```

### Docker Compose (Legacy)
```bash
make cdc-verify
```

---

## 7.8 Lưu ý

1. **postgres-init và cdc-seed dùng Python** psycopg2 — không cần cài postgresql-client
2. **Service names K8s**: `svc-postgres-cdc`, `svc-debezium` (prefix `svc-`)
3. **Snapshot mode = never**: Trong pipeline này, Postgres đã có data sẵn từ cdc-seed
4. **Unique consumer group**: Bridge dùng UUID-based group để không miss events:
   ```python
   group_id=f"cdc-bridge-{uuid4().hex[:8]}"
   ```
5. **Async optimization**: `producer.send()` + `flush()` mỗi 500 events giúp throughput gấp ~50x so với sync



# 8. Apache Superset — Dashboard và Visualization

## 8.1 Tổng quan

Apache Superset 4.0.0 cung cấp giao diện trực quan cho dữ liệu NYC Taxi, 
kết nối đến Trino để query các mart views.

### Cấu hình kết nối
- **Database**: Trino (SQLAlchemy URI: `trino://analytics@svc-trino:8080/hive/mart`)
- **Schema**: `hive.mart` (dbt views)
- **Thông tin**: Truy cập qua `http://localhost:39080` (K8s) hoặc `http://localhost:8088` (Docker)
- **Credentials**: `admin` / `admin`

---

## 8.2 Bootstrap Script

**Script**: `scripts/superset_bootstrap.py`

Script idempotent tự động:
1. Đăng nhập Superset (REST API)
2. Tạo/kiểm tra Database connection đến Trino
3. Tạo/kiểm tra 7 datasets
4. Tạo/kiểm tra 4 charts
5. Tạo/kiểm tra 1 dashboard

### Chi tiết bootstrap

```python
BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088") + "/api/v1"
TRINO_URI = os.environ.get("TRINO_URI", 
    "trino://analytics@trino-coordinator:8080/hive/mart")
```

### 1. Database
```python
dbs = get("/database/")
# Tìm "NYC Trino" hoặc tạo mới
resp = post("/database/", {
    "database_name": "NYC Trino",
    "sqlalchemy_uri": TRINO_URI
})
```

### 2. Datasets (7 tables từ hive.mart)

| Dataset | Table | Ghi chú |
|---------|-------|---------|
| fact_trips | `hive.mart.fact_trips` | Fact table chính |
| dim_zone | `hive.mart.dim_zone` | Zone dimension |
| mart_hourly_summary | `hive.mart.mart_hourly_summary` | Hourly aggregation |
| mart_payment_type_summary | `hive.mart.mart_payment_type_summary` | Payment type summary |
| mart_revenue_by_day | `hive.mart.mart_revenue_by_day` | Daily revenue |
| mart_revenue_by_zone | `hive.mart.mart_revenue_by_zone` | Revenue by zone |
| gold_fact_trips | `hive.mart.gold_fact_trips` | Gold fact table |

### 3. Charts (4 charts)

| Chart | Viz Type | Dataset | Mô tả |
|-------|----------|---------|-------|
| `trips_per_hour` | bar | fact_trips | Số chuyến theo giờ |
| `top_pickup_zones` | table | fact_trips | Top zones đón khách |
| `borough_revenue` | bar | fact_trips | Doanh thu theo borough |
| `daily_trips` | line | fact_trips | Xu hướng chuyến đi theo ngày |

### 4. Dashboard
- **Title**: "NYC Taxi Overview"
- **Slug**: `nyc-taxi`
- **Published**: true

---

## 8.3 Superset Entrypoint

**File**: `docker/superset/entrypoint-superset.sh`

```bash
# 1. Wait for Trino ready
for i in {1..60}; do
  if curl -sf http://trino-coordinator:8080/v1/info >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# 2. Install SQLAlchemy Trino driver
pip install sqlalchemy-trino -q

# 3. Initialize DB + create admin user
superset db upgrade
superset fab create-admin \
  --username admin --password admin --role Admin

# 4. Init roles + permissions
superset init

# 5. Start webserver (background) + bootstrap
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger &
# Wait for webserver ready, then register DB/charts/dashboard
bash /app/docker/bootstrap_superset.sh
```

### Superset Config

**File**: `docker/superset/superset_config.py`
```python
# Disable CSRF cho bootstrap script (POST requests)
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
ENABLE_CSP = False
```

---

## 8.4 Bootstrap Script

### Kubernetes (Airflow DAG) ⭐

Airflow tự động chạy Superset bootstrap trong `nyc_e2e_pipeline` và `nyc_analytics_refresh`:

```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    cmds=["python3"],
    arguments=["/opt/project/scripts/superset_bootstrap.py"],
    env_vars=[
        ("SUPERSET_URL", "http://svc-superset:8088"),
        ("TRINO_URI", "trino://analytics@svc-trino:8080/hive/mart"),
    ],
)
```

### Docker Compose (Legacy)
```bash
make superset-bootstrap
# Thực thi: docker exec -i nyc_superset python3 < scripts/superset_bootstrap.py
```

---

## 8.5 Analytics Questions (SQL)

**File**: `sql/analytics_questions.sql` — 10 business questions chạy trên Trino.

**Script**: `scripts/run_analytics_questions.py`

### Danh sách 10 câu hỏi

| # | Câu hỏi | SQL |
|---|---------|-----|
| 1 | Top 10 pickup zones by trips | `SELECT pickup_zone, COUNT(*) AS n FROM hive.mart.fact_trips GROUP BY ... ORDER BY n DESC LIMIT 10` |
| 2 | Hourly distribution | `SELECT pickup_hour, COUNT(*), AVG(total_amount) FROM hive.mart.fact_trips GROUP BY pickup_hour` |
| 3 | Borough-to-borough matrix | `SELECT pickup_borough, dropoff_borough, COUNT(*) FROM hive.mart.fact_trips ... LIMIT 20` |
| 4 | Average fare by payment type | `SELECT payment_type, COUNT(*), AVG(fare_amount) FROM hive.mart.fact_trips GROUP BY payment_type` |
| 5 | Daily gross revenue | `SELECT pickup_date, COUNT(*), SUM(total_amount) FROM hive.mart.fact_trips GROUP BY pickup_date` |
| 6 | Top 10 longest trips | `SELECT pickup_ts, trip_distance, total_amount, ... FROM hive.mart.fact_trips ORDER BY trip_distance DESC LIMIT 10` |
| 7 | Top 5 boroughs by revenue | `SELECT pickup_borough, SUM(total_amount), AVG(tip_rate) FROM hive.mart.fact_trips ... LIMIT 5` |
| 8 | Hourly summary via mart | `SELECT * FROM hive.mart.mart_hourly_summary ORDER BY pickup_date, pickup_hour` |
| 9 | Mart inventory | `SELECT table_name, table_type FROM hive.information_schema.tables WHERE table_schema = 'mart'` |
| 10 | Invalid trips view check | `SELECT 'invalid_trips_view_resolves', COUNT(*) FROM hive.mart.fact_invalid_trips` |

### Validation

Script kiểm tra mỗi query trả về ≥1 row. Kỳ vọng **10/10 PASS**.

```python
def main():
    questions = split_questions(raw_sql)
    for i, q in enumerate(questions, 1):
        cur.execute(q)
        rows = cur.fetchall()
        n = len(rows)
        if n == 0:
            failures.append((i, "zero rows"))
    if failures:
        return 1  # FAIL
    return 0  # PASS (10/10)
```

**Output mẫu:**
```
[analytics] 10 questions found in analytics_questions.sql
[Q1] 2573 rows in 1.23s | first: ('Manhattan', 2573)
[Q2]   24 rows in 0.45s | first: (0, 123, 45.67)
...
[analytics] PASS 10/10
```

---

## 8.6 UIs & Port-forwards

### Kubernetes (Skaffold) ⭐

| Service | URL | Credentials |
|---------|-----|-------------|
| **Superset** | http://localhost:39080 | `admin` / `admin` |
| MinIO Console | http://localhost:39086 | `minio` / `minio123` |
| Kafka UI | http://localhost:39082 | — |
| Spark Master | http://localhost:39083 | — |
| Trino | http://localhost:39084 | — |
| Airflow | http://localhost:39085 | `admin` / `admin` |

Port-forward tự động qua Skaffold. Nếu cần thủ công:
```bash
./scripts/k8s_ui.sh start
```

### Docker Compose (Legacy)
| Service | URL | Credentials |
|---------|-----|-------------|
| Superset | http://localhost:8088 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minio` / `minio123` |
| Airflow | http://localhost:8085 | `admin` / `admin` |
| Kafka UI | http://localhost:8080 | — |



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



# 10. Helm Chart và Skaffold Deployment

## 10.1 Tổng quan

Trên Kubernetes, pipeline được triển khai qua **Helm chart** (`charts/nyc-taxi/`) 
và **Skaffold** (`skaffold.yaml`). Skaffold là công cụ triển khai chính, 
tự động build images, sync files, và port-forward.

### Skaffold vs Makefile (K8s)

| Khía cạnh | Skaffold (Primary) | Makefile (Legacy) |
|-----------|-------------------|-------------------|
| Deploy | `skaffold dev` / `skaffold run` | `make k8s-deploy` |
| File sync | Auto (sync rules) | Manual (tar + docker exec) |
| Port-forward | Auto (portForward config) | `make k8s-ui` (setsid -f) |
| Build | Skaffold build artifacts | `make k8s-images` (docker build + kind load) |
| Watch | Auto-rebuild on Dockerfile changes | None |
| Hot-reload | Sync rules + file-sync pod | None |

---

## 10.2 Skaffold Configuration

**File**: `skaffold.yaml`

```yaml
apiVersion: skaffold/v4beta3
kind: Config
```

### Build Section
```yaml
build:
  local:
    push: false  # Không push lên registry (local kind cluster)
  artifacts:
    - image: nyc-pipeline-tools
      context: .
      docker: { dockerfile: docker/tools.Dockerfile }
      sync:
        manual:
          - src: "airflow/dags/**/*.py"
            dest: /opt/project/airflow/dags/
          - src: "jobs/**/*"
            dest: /opt/project/jobs/
          - src: "scripts/**/*"
            dest: /opt/project/scripts/
          - src: "dbt/**/*"
            dest: /opt/project/dbt/
          - src: "charts/**/*"
            dest: /opt/project/charts/
    - image: nyc-dbt
      context: .
      docker: { dockerfile: docker/dbt.Dockerfile }
    - image: nyc-airflow
      context: .
      docker: { dockerfile: docker/airflow.Dockerfile }
```

**3 artifacts** được build:
1. `nyc-pipeline-tools` — Có sync rules cho hot-reload
2. `nyc-dbt` — Không sync (ít thay đổi)
3. `nyc-airflow` — Không sync

### Deploy Section
```yaml
deploy:
  helm:
    releases:
      - name: nyc-taxi
        chartPath: charts/nyc-taxi
        namespace: nyc-taxi
        valuesFiles: [charts/nyc-taxi/values.yaml]
        createNamespace: true
```

### Hooks

**Before hook** (chạy trước khi Helm deploy):
```yaml
before:
  - host:
      command:
        - bash -c
        - |
          # 1. Xoá immutable jobs cũ
          kubectl delete job -n nyc-taxi --all --ignore-not-found
          # 2. Sync project files → kind-worker PVC
          docker exec kind-worker mkdir -p /mnt/nyc-project
          tar cf - \
            --exclude='dbt/logs' --exclude='dbt/target' \
            --exclude='.git' --exclude='__pycache__' \
            airflow/dags/ jobs/ scripts/ dbt/ charts/ \
            | docker exec -i kind-worker tar xf - -C /mnt/nyc-project
```

**After hook** (chạy sau Helm deploy):
```yaml
after:
  - host:
      command:
        - bash -c
        - |
          sleep 5
          echo "=== UIs ==="
          echo "  Superset     http://localhost:39080"
          echo "  MinIO API    http://localhost:39081"
          # ... hiển thị tất cả URLs
          # Mở browser tabs
          xdg-open http://localhost:39085 2>/dev/null || true
```

### Port-forward Section
```yaml
portForward:
  - resourceType: service
    resourceName: svc-superset
    port: 8088
    localPort: 39080
  - resourceType: service
    resourceName: svc-minio
    port: 9000
    localPort: 39081
  - resourceType: service
    resourceName: svc-minio
    port: 9001
    localPort: 39086
  - resourceType: service
    resourceName: svc-kafka-ui
    port: 8080
    localPort: 39082
  - resourceType: service
    resourceName: svc-spark-master
    port: 8081
    localPort: 39083
  - resourceType: service
    resourceName: svc-trino
    port: 8080
    localPort: 39084
  - resourceType: service
    resourceName: svc-airflow-webserver
    port: 8080
    localPort: 39085
  - resourceType: service
    resourceName: svc-postgres-cdc
    port: 5432
    localPort: 39087
```

---

## 10.3 Helm Chart Structure

```mermaid
mindmap
  root((📦 nyc-taxi Helm Chart))
    Chart.yaml
    values.yaml
    templates
      namespace
        namespace.yaml
      storage
        project-files-pv.yaml
        raw-data-pv.yaml
      zookeeper
        statefulset.yaml
        service.yaml
      kafka
        statefulset.yaml
        service.yaml
      kafka-ui
        deployment.yaml
        service.yaml
      minio
        deployment.yaml
        service.yaml
        pvc.yaml
      spark
        master-deployment.yaml
        master-service.yaml
        worker-deployment.yaml
        worker-service.yaml
      postgres-cdc
        statefulset.yaml
        service.yaml
        pvc.yaml
      debezium
        deployment.yaml
        service.yaml
      trino
        deployment.yaml
        service.yaml
        configmap.yaml
      superset
        deployment.yaml
        service.yaml
        configmap.yaml
      airflow
        postgres
          statefulset.yaml
          service.yaml
          pvc.yaml
        rbac.yaml
        init-job.yaml
        file-sync.yaml
        scheduler
          deployment.yaml
        webserver
          deployment.yaml
          service.yaml
      dbt
        (empty - runs via Airflow KPO)
      jobs
        minio-setup.yaml
        postgres-init.yaml
        topic-init.yaml
```

### Chart.yaml
```yaml
apiVersion: v2
name: nyc-taxi
description: A Helm chart for NYC Taxi pipeline
type: application
version: 0.1.0
appVersion: "1.0.0"
```

### Service Naming Convention

Tất cả service names đều có prefix `svc-`:
```
svc-kafka, svc-minio, svc-trino, svc-superset,
svc-zookeeper, svc-spark-master, svc-spark-worker,
svc-kafka-ui, svc-postgres-cdc, svc-debezium,
svc-airflow-webserver, svc-airflow-scheduler
```

> ⚠️ **Quan trọng**: Trong K8s namespace `nyc-taxi`, dùng `svc-kafka:9092` 
> chứ **không phải** `kafka:9092`.

---

## 10.4 Key Templates

### Storage (PVC + PV)

**File**: `charts/nyc-taxi/templates/storage/project-files-pv.yaml`

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: project-files-pv
spec:
  capacity:
    storage: 5Gi
  accessModes:
  - ReadWriteOnce
  hostPath:
    path: /mnt/nyc-project
    type: Directory
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - kind-worker
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: project-files-pvc
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: ""
```

**Đặc điểm:**
- 2 PVs: `project-files-pv` (5Gi) + `raw-data-pv` (tùy data size)
- hostPath → `/mnt/nyc-*` trên kind-worker node
- Node affinity: chỉ mount trên `kind-worker` (RWO)

### File-Sync Pod

**File**: `charts/nyc-taxi/templates/airflow/file-sync.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: file-sync
spec:
  replicas: 1
  selector:
    matchLabels:
      app: file-sync
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: kind-worker
      containers:
      - name: sync
        image: nyc-pipeline-tools:k8s
        command: ["sleep", "infinity"]
        volumeMounts:
        - name: project-files
          mountPath: /opt/project
      volumes:
      - name: project-files
        persistentVolumeClaim:
          claimName: project-files-pvc
```

**Vai trò**: Là target cho `skaffold sync`. Khi file thay đổi, 
Skaffold push file vào pod này → PVC → tất cả pods khác thấy thay đổi.

### MinIO Setup Job

**File**: `charts/nyc-taxi/templates/jobs/minio-setup.yaml`

Job one-shot dùng `minio/mc` để:
1. Chờ MinIO ready
2. Tạo buckets: nyc-raw, nyc-silver, nyc-quarantine, nyc-lookup, nyc-gold
3. Upload raw Parquet + zone lookup CSV

### Topic Init Job

**File**: `charts/nyc-taxi/templates/jobs/topic-init.yaml`

Dùng `nyc-pipeline-tools:k8s` image với `entrypoint-topic-init`:
- Chờ Kafka ready (qua wait-kafka)
- Tạo topics: taxi.trip.events, taxi.trip.invalid, taxi.trip.dlq

### Postgres Init Job

**File**: `charts/nyc-taxi/templates/jobs/postgres-init.yaml`

Dùng `nyc-pipeline-tools:k8s` image với `entrypoint-init-postgres`:
- Chờ Postgres ready
- Tạo trips table
- Set REPLICA IDENTITY FULL

---

## 10.5 Skaffold Dev Workflow

```bash
# Full dev loop (build + deploy + sync + port-forward + watch)
skaffold dev --namespace nyc-taxi

# Output:
# [1/3] Building nyc-pipeline-tools...
# [2/3] Building nyc-dbt...
# [3/3] Building nyc-airflow...
# [helm] Deploying nyc-taxi chart...
# [port-forward] Forwarding svc-superset:8088 → 39080
# ...
# Watching for changes...
```

Khi chạy `skaffold dev`:
1. Build 3 artifacts (nếu Dockerfile thay đổi)
2. Run pre-deploy hooks (xóa jobs, sync files)
3. Helm install/upgrade chart
4. Start port-forwards (39080-39087)
5. Watch file changes:
   - `airflow/dags/`, `jobs/`, `scripts/`, `dbt/`, `charts/` → sync vào file-sync pod
   - Dockerfile thay đổi → rebuild image + re-deploy
6. Run post-deploy hooks (hiển thị URLs)

### Skaffold Commands

```bash
# Dev mode (auto-watch + auto-sync)
skaffold dev --namespace nyc-taxi

# One-shot (no watch)
skaffold run --namespace nyc-taxi

# Build only
skaffold build --namespace nyc-taxi

# Deploy only (skip build)
skaffold run --namespace nyc-taxi --tail

# Delete deployed resources
skaffold delete --namespace nyc-taxi
```

---

## 10.6 PVC Sync (Manual)

Khi không dùng skaffold, sync files thủ công:

```bash
cd /home/dwcks/vsf_gsm/nyc_new
tar cf - \
  --exclude='dbt/logs' --exclude='dbt/target' \
  --exclude='.git' --exclude='__pycache__' \
  --exclude='*.pyc' --exclude='*.pyo' \
  airflow/dags/ jobs/ scripts/ dbt/ charts/ \
  | docker exec -i kind-worker tar xf - -C /mnt/nyc-project
```

---

## 10.7 Legacy Makefile K8s Targets

| Target | Description |
|--------|-------------|
| `k8s-cluster` | Create kind cluster (3 nodes) |
| `k8s-images` | Build + load images into kind |
| `k8s-deploy` | Deploy all K8s manifests (ordered) |
| `k8s-start` | Cluster → images → services → UIs |
| `k8s-up` | Full: cluster check + build + deploy + wait + UI |
| `k8s-down` / `k8s-stop` | Scale down / stop services |
| `k8s-destroy` | Delete cluster (all data gone) |
| `k8s-ui` | Start port-forwards (setsid -f) |
| `k8s-ui-stop` | Stop port-forwards |
| `k8s-verify` | Row counts via Trino |
| `k8s-verify-analytics` | 10 analytics queries |
| `k8s-verify-cdc` | CDC pipeline verification |
| `k8s-clean` | Clean MinIO data + delete jobs |
| `k8s-status` | kubectl get pods |
| `k8s-logs JOB=name` | Tail job logs |

---

## 10.8 Troubleshooting Namespace

Khi namespace `nyc-taxi` bị stuck ở trạng thái `Terminating`:

```bash
kubectl replace --raw /api/v1/namespaces/nyc-taxi/finalize -f \
  <(kubectl get namespace nyc-taxi -o json | python3 -c \
    "import json,sys; ns=json.load(sys.stdin); ns['spec']['finalizers']=[]; print(json.dumps(ns))")
```



# 11. Scripts và Tiện Ích

## 11.1 Tổng quan

Thư mục `scripts/` chứa 24 utility scripts phục vụ nhiều mục đích:
- Pipeline operations (Trino, CDC, Spark streaming)
- Verification (mart counts, analytics, CDC)
- Bootstrap (Superset datasets/charts/dashboard)
- Data management (gold export, partition sync)
- DevOps (port-forward, download, local E2E)

---

## 11.2 Trino Scripts

### trino_register.py

**Mục đích**: Đăng ký Hive external tables trong Trino catalog.

#### Kubernetes (Skaffold/Airflow) ⭐
Chạy qua `entrypoint-trino-bootstrap` trong KubernetesPodOperator:
```bash
# Airflow tự động chạy task trino_bootstrap
kubectl exec -n nyc-taxi -it deploy/trino -- trino --user analytics
```

#### Docker Compose (Legacy)
```bash
python3 scripts/trino_register.py
# ENV: TRINO_HOST, TRINO_PORT, S3_MODE, SILVER_PATH, ...
```

**Luồng xử lý:**
1. Chờ Trino ready (TCP connect, timeout 120s)
2. CREATE SCHEMA IF NOT EXISTS hive.nyc
3. Với mỗi table:
   - DROP TABLE IF EXISTS
   - CREATE TABLE WITH (external_location, format)
4. Sync partitions cho trips table
5. Smoke test (SELECT COUNT(*))

**Tables được tạo:**
- `hive.nyc.trips` (partitioned by pickup_year, pickup_month)
- `hive.nyc.invalid_trips` (non-partitioned)
- `hive.nyc.taxi_zone_lookup` (CSV external)

### trino_sync_partitions.py

**Mục đích**: Sync partition metadata trong Trino Hive catalog.

```bash
python3 scripts/trino_sync_partitions.py
```

Chạy:
```sql
CALL hive.system.sync_partition_metadata(
  schema_name => 'nyc',
  table_name => 'trips',
  mode => 'FULL'
)
```

Được gọi tự động trong `entrypoint-dbt.sh` trước khi chạy dbt build.

### export_gold_to_minio.py

**Mục đích**: Export datasets từ Trino sang MinIO S3 dưới dạng Parquet.

```bash
python3 scripts/export_gold_to_minio.py
# ENV: TRINO_HOST, TRINO_PORT, MINIO_ENDPOINT, ...
```

Xem chi tiết tại **docs/05-trino-catalog.md** (Section 5.4).

---

## 11.3 CDC Scripts

### cdc_seed.py

**Mục đích**: Đọc raw Parquet và insert vào Postgres trips table.

```bash
python3 scripts/cdc_seed.py \
  --input /opt/project/data/raw/.../yellow_tripdata_2024-01.parquet \
  --max-rows 5000 \
  --dsn postgresql://postgres:postgres@svc-postgres-cdc:5432/nyc_taxi
```

**Chi tiết:**
- Đọc Parquet bằng Pandas
- Map columns: VendorID → vendor_id, tpep_pickup_datetime → pickup_datetime...
- Insert qua SQLAlchemy (chunk-based)
- Mặc định 5000 rows

### cdc_register_connector.py

**Mục đích**: Đăng ký Debezium Postgres connector qua REST API.

```bash
python3 scripts/cdc_register_connector.py \
  --debezium-url http://svc-debezium:8083 \
  --postgres-host svc-postgres-cdc
```

**Chi tiết:**
- POST connector config lên `${debezium_url}/connectors`
- Config gồm pgoutput plugin, ExtractNewRecordState transform
- Idempotent: DELETE + POST

### cdc_bridge.py

**Mục đích**: Bridge Debezium CDC events → standard taxi.trip.events format.

```bash
python3 scripts/cdc_bridge.py \
  --bootstrap-server svc-kafka:9092 \
  --input-topic nyc_cdc.public.trips \
  --output-topic taxi.trip.events
```

**Chi tiết:**
- Poll-based consumer loop với idle timeout
- Async Kafka send + periodic flush (500 events)
- Transform: unwrap Debezium envelope → flat event format
- Benchmark output at exit (events, time, throughput)

Xem chi tiết tại **docs/07-cdc-pipeline.md**.

---

## 11.4 Kafka Scripts

### create_kafka_topics.py

**Mục đích**: Tạo Kafka topics cho pipeline.

```bash
python3 scripts/create_kafka_topics.py \
  --bootstrap-server svc-kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

**Topics được tạo:**
| Topic | Partitions | Mục đích |
|-------|-----------|----------|
| `taxi.trip.events` | 3 | Main event stream |
| `taxi.trip.invalid` | 3 | Invalid events |
| `taxi.trip.dlq` | 3 | Dead letter queue |

### create_per_run_topic.py

**Mục đích**: Tạo topic tạm thời cho một run cụ thể.

```bash
python3 scripts/create_per_run_topic.py <bootstrap-server> <topic> <partitions>
```

Dùng cho các run testing với topic riêng.

### create_kafka_topics.sh

Shell wrapper cho `create_kafka_topics.py` (legacy).

---

## 11.5 Spark Streaming Scripts

### start_streaming_job.sh

**Mục đích**: Submit Spark streaming job từ host.

```bash
# Usage:
TOPIC=taxi.trip.events bash scripts/start_streaming_job.sh
```

K8s mode: `spark-submit` với master `spark://svc-spark-master:7077`.

### start_streaming_job_docker.sh

**Mục đích**: Submit Spark streaming job trong Docker Compose mode.

```bash
# Usage (từ Makefile):
TOPIC=taxi.trip.events bash scripts/start_streaming_job_docker.sh
```

Docker mode: `docker run` với `--network nyc_new_default`.

---

## 11.6 Verification Scripts

### run_analytics_questions.py

**Mục đích**: Chạy 10 analytics SQL queries, kiểm tra mỗi query trả về ≥1 row.

```bash
python3 scripts/run_analytics_questions.py
# ENV: TRINO_HOST, TRINO_PORT
```

**Output mẫu:**
```
[analytics] 10 questions found in analytics_questions.sql
[Q1] 2573 rows in 1.23s | first: ('Manhattan', 2573)
...
[analytics] PASS 10/10
```

Xem chi tiết tại **docs/08-superset-visualization.md** (Section 8.5).

### verify_mart.py

**Mục đích**: Đếm rows của 4 mart tables quan trọng.

```bash
python3 scripts/verify_mart.py
# ENV: TRINO_HOST, TRINO_PORT
```

**Tables verified:**
| Table | Expected rows |
|-------|--------------|
| `dim_zone` | ~261 |
| `fact_trips` | ~8-10M |
| `mart_hourly_summary` | ~11K+ |
| `mart_revenue_by_day` | ~90-96 |

### superset_check.py

**Mục đích**: Liệt kê tất cả resources trong Superset.

```bash
python3 scripts/superset_check.py
# ENV: SUPERSET_URL
```

### superset_dashboard_update.py / update_dashboard.py

**Mục đích**: Update dashboard configuration programmatically.

```bash
python3 scripts/superset_dashboard_update.py
```

---

## 11.7 Bootstrapping Scripts

### superset_bootstrap.py

**Mục đích**: Idempotent Superset setup — DB, datasets, charts, dashboard.

```bash
python3 scripts/superset_bootstrap.py
# ENV: SUPERSET_URL, TRINO_URI
```

Xem chi tiết tại **docs/08-superset-visualization.md**.

### run_dbt.sh

**Mục đích**: Shell wrapper cho dbt commands.

```bash
bash scripts/run_dbt.sh [build|run|test]
```

---

## 11.8 Data & E2E Scripts

### download_data.sh

**Mục đích**: Download dữ liệu NYC TLC (Parquet) từ nguồn.

```bash
bash scripts/download_data.sh
```

### local_e2e_full_9_5m.sh

**Mục đích**: Chạy full E2E pipeline local (9.5M rows).

```bash
bash scripts/local_e2e_full_9_5m.sh
```

### local_e2e_test.sh

**Mục đích**: Chạy E2E test nhanh local.

```bash
bash scripts/local_e2e_test.sh
```

---

## 11.9 DevOps Script

### k8s_ui.sh

**Mục đích**: Quản lý K8s port-forwards với auto-restart.

```bash
./scripts/k8s_ui.sh start   # Start port-forwards (setsid -f)
./scripts/k8s_ui.sh stop    # Stop all port-forwards
```

**Cơ chế:**
```bash
for mapping in "svc/svc-superset:39080:8088" "svc/svc-minio:39081:9000" ...; do
    setsid -f sh -c "
      while true; do
        kubectl port-forward --address 0.0.0.0 -n nyc-taxi $svc $lport:$rport > /dev/null 2>&1
        sleep 3  # Auto-restart if connection lost
      done
    "
done
```

**Đặc điểm:**
- Dùng `setsid -f` — process sống sau khi `make` thoát
- `--address 0.0.0.0` — cho phép truy cập từ máy khác
- Auto-restart loop khi connection lost
- Port range 39080-39087 (tránh conflict với kind NodePort 38080)

---

## 11.10 Run Generator Scripts

### run_generator.sh

**Mục đích**: Chạy Kafka producer generator (sinh events từ Parquet).

```bash
bash scripts/run_generator.sh
```

### run_generator_full.sh

**Mục đích**: Chạy generator với tất cả dữ liệu.

```bash
bash scripts/run_generator_full.sh
```

---

## 11.11 Script Dependencies Overview

```
scripts/
├── trino_register.py              # trino (pip)
├── trino_sync_partitions.py        # trino
├── export_gold_to_minio.py         # trino, minio (pip)
├── run_analytics_questions.py      # trino
├── verify_mart.py                  # trino
├── superset_bootstrap.py           # urllib (stdlib)
├── superset_check.py               # urllib
├── superset_dashboard_update.py    # urllib
├── cdc_bridge.py                   # kafka-python
├── cdc_seed.py                     # pandas, pyarrow, sqlalchemy
├── cdc_register_connector.py       # urllib
├── create_kafka_topics.py          # kafka-python
├── create_per_run_topic.py         # kafka-python
├── k8s_ui.sh                       # bash + kubectl
├── download_data.sh                # bash (wget/curl)
├── local_e2e_*.sh                  # bash (make targets)
├── run_generator*.sh               # bash
└── run_dbt.sh                      # bash (dbt CLI)
```



# 12. Luồng Dữ Liệu và Storage

## 12.1 Data Lake Structure (MinIO S3)

```mermaid
flowchart LR
    subgraph MINIO["MinIO S3 (http://minio:9000 | minio/minio123)"]
        direction TB
        RAW["nyc-raw/<br/>Raw Parquet<br/>~153MB"]
        SILVER["nyc-silver/trips/<br/>Enriched + Valid<br/>~265MB<br/>Partitioned by year/month"]
        QUAR["nyc-quarantine/<br/>Invalid trips<br/>~36MB"]
        LOOKUP["nyc-lookup/<br/>taxi_zone_lookup.csv<br/>~12KB, 265 zones"]
        GOLD["nyc-gold/<br/>30+ datasets<br/>Gold exports"]
    end

    RAW -->|Spark Batch| SILVER
    RAW -->|Spark Batch| QUAR
    SILVER -->|Trino CTAS| GOLD
```

### Bucket Sizes

| Bucket | Size | Records | Description |
|--------|------|---------|-------------|
| `nyc-raw` | ~153 MB | 3 files (3 tháng) | Raw Parquet |
| `nyc-silver` | ~265 MB | ~10.2M valid trips | Enriched + validated |
| `nyc-quarantine` | ~36 MB | ~1.07M invalid | Dữ liệu lỗi |
| `nyc-lookup` | ~12 KB | 265 zones | Zone lookup CSV |
| `nyc-gold` | varies | 30+ datasets | Gold exports |

---

## 12.2 Data Flow Diagram (Chi tiết)

### Batch Path
```
┌─────────────────────────────────────────────────────────────────────┐
│ BATCH PATH (@monthly backfill)                                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. MINIO-SETUP (job one-shot)                                      │
│     ├── Tạo buckets: nyc-raw, nyc-silver, nyc-quarantine, ...       │
│     ├── Upload raw Parquet từ PVC → nyc-raw/                        │
│     └── Upload taxi_zone_lookup.csv → nyc-lookup/                    │
│                                                                     │
│  2. SPARK BATCH (spark_local_batch.py)                              │
│     ├── Đọc: s3a://nyc-raw/yellow_taxi/.../*.parquet                │
│     ├── Đọc: s3a://nyc-lookup/taxi_zone_lookup.csv                  │
│     ├── Enrich: cast types, join zones, add metadata                │
│     ├── Validate: 10 rules                                          │
│     └── Ghi:                                                        │
│         ├── Valid   → s3a://nyc-silver/trips/ (partitioned)         │
│         └── Invalid → s3a://nyc-quarantine/invalid_trips/           │
│                                                                     │
│  3. TRINO BOOTSTRAP (trino_register.py)                             │
│     └── Register external tables → hive.nyc.{trips,invalid_trips,   │
│                                         taxi_zone_lookup}           │
│                                                                     │
│  4. DBT BUILD (dbt build)                                           │
│     ├── stg_trips, stg_zones, stg_invalid_trips                    │
│     ├── fact_trips, dim_zone, fact_invalid_trips                   │
│     ├── mart_hourly_summary, mart_revenue_by_day, ...              │
│     ├── gold_fact_trips, gold_dim_zone, ...                        │
│     └── 9 tests (expect 24/24 PASS)                                │
│                                                                     │
│  5. GOLD EXPORT (export_gold_to_minio.py)                           │
│     └── CTAS: hive.nyc_gold.* → s3://nyc-gold/{name}/              │
│                                                                     │
│  6. SUPERSET BOOTSTRAP (superset_bootstrap.py)                      │
│     └── Register DB + 7 datasets + 4 charts + dashboard            │
│                                                                     │
│  7. ANALYTICS CHECK (run_analytics_questions.py)                    │
│     └── 10 SQL queries, expect PASS 10/10                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Streaming Path

```mermaid
flowchart TD
    subgraph INPUT["1. Kafka Producer / CDC Bridge"]
        IN["Send JSON events → taxi.trip.events topic"]
    end

    subgraph SS["2. Spark Streaming (spark_stream_taxi_events.py)"]
        READ_S["ReadStream từ Kafka<br/>startingOffsets=earliest"]
        PARSE["Parse JSON → EVENT_SCHEMA<br/>from_json(col('value'))"]
        EV["Enrich + Validate<br/>(cùng logic với batch)"]
        FB["foreachBatch:<br/>persist + filter valid/invalid + write"]
        READ_S --> PARSE --> EV --> FB
    end

    subgraph OUTPUT_S["Output S3"]
        VS["✅ Valid → s3a://nyc-silver/trips/<br/>partitioned append"]
        IS["❌ Invalid → s3a://nyc-quarantine/"]
    end

    subgraph REST["3. Trino + dbt + Superset (same as batch)"]
        SAME["(cùng luồng với batch path)"]
    end

    INPUT --> SS
    FB --> VS
    FB --> IS
    VS --> REST
```

### CDC Path

```mermaid
flowchart TD
    subgraph SEED["1. Seed Postgres"]
        CS["cdc_seed.py<br/>5000 rows từ Parquet → nyc_postgres.trips"]
    end

    subgraph REG["2. Register Debezium"]
        CR["cdc_register_connector.py<br/>POST connector → http://debezium:8083<br/>Capture changes từ WAL"]
    end

    subgraph BRIDGE_PHASE["3. CDC Bridge"]
        CONS["Consumer: nyc_cdc.public.trips<br/>(raw Debezium envelope)"]
        TRANS["Transform: extract fields, format timestamps"]
        PROD["Producer: taxi.trip.events<br/>(standard format)"]
        CONS --> TRANS --> PROD
    end

    subgraph SS["4. Spark Streaming"]
        SS2["(cùng logic với batch path)"]
    end

    SEED --> REG --> BRIDGE_PHASE --> SS
```

---

## 12.3 Local Filesystem Structure

### Kubernetes PVC Structure (Primary) ⭐
```
kind-worker:/mnt/
├── nyc-project/      # project-files-pv (5Gi) — code + config
│   ├── airflow/dags/ # Airflow DAGs (hot-reload qua skaffold sync)
│   ├── jobs/         # Spark jobs
│   ├── scripts/      # Utility scripts
│   ├── dbt/          # dbt models + profiles
│   └── charts/       # Helm chart
│
└── nyc-data/         # raw-data-pv — dữ liệu đầu vào
    ├── raw/yellow_taxi/...
    └── lookup/taxi_zone_lookup.csv
```

### Local (Docker Compose — Legacy)
```
data/
├── raw/yellow_taxi/...
├── silver/trips/...
├── quarantine/invalid_trips/
├── lookup/taxi_zone_lookup.csv
├── checkpoints/spark_stream_taxi_events/
└── trino-metastore/  # Hive metastore (file-based)
```

---

## 12.4 Partition Strategy

### Silver Trips (Hive-style partitioning)

```mermaid
flowchart LR
    subgraph S3["s3a://nyc-silver/trips/"]
        Y2024["pickup_year=2024/"]
        Y2024 --> M1["pickup_month=1/"]
        Y2024 --> M2["pickup_month=2/"]
        Y2024 --> M3["pickup_month=3/"]
        M1 --> P1["part-xxx.snappy.parquet"]
        M2 --> P2["part-xxx.snappy.parquet"]
    end
```

**Partition columns**: `pickup_year` (INT), `pickup_month` (INT)
**Lợi ích**: 
- Query pruning (Trino chỉ đọc partitions cần thiết)
- dbt staging giữ nguyên partition columns
- Gold export giữ nguyên partitioning

### Quarantine Invalid Trips (Non-partitioned)

```mermaid
flowchart LR
    QUAR["s3a://nyc-quarantine/invalid_trips/"] --> P3["part-xxx.snappy.parquet"]
```

**Không partition** vì số lượng invalid trips nhỏ hơn nhiều.

---

## 12.5 Data Volume by Stage

| Stage | Volume | Tỉ lệ |
|-------|--------|-------|
| Raw (3 tháng, 8 files) | ~153 MB | 100% |
| Silver (valid) | ~265 MB | ~89% |
| Quarantine (invalid) | ~36 MB | ~11% |
| Gold (30+ datasets) | ~500 MB+ | ~170%+ |

**Invalid rate**: ~11.24% (1.07M invalid / 9.55M total)

### Invalid Breakdown
| Lỗi | Số lượng (ước tính) |
|-----|-------------------|
| Invalid passenger count | ~30% |
| Non-positive trip distance | ~25% |
| Invalid datetime / duration | ~20% |
| Payment type out of range | ~15% |
| Other | ~10% |

---

## 12.6 Key Storage Notes (K8s/Skaffold)

1. **Spark dùng `s3a://`** — Hadoop S3A connector với `--packages`
2. **Trino dùng `s3://`** — Hive S3 connector native
3. **MinIO path-style access** — Bắt buộc cho MinIO (virtual-host không support)
4. **PVC project-files-pv** — 5Gi hostPath, nodeAffinity: kind-worker
5. **File-based Hive metastore** — Trong container Trino, không cần Hive service riêng
6. **Streaming checkpoint** — Trên S3 cho K8s (`s3a://nyc-silver/checkpoints/...`)
7. **mode("append")** — Luôn dùng append, không overwrite
8. **File-sync hot-reload** — Skaffold watch + file-sync pod → PVC → tất cả pods



# 13. Tổng Kết Kiến Trúc

## 13.1 Pipeline Components Summary

```mermaid
flowchart TD
    subgraph SOURCE["📥 Data Sources"]
        RP["Raw Parquet (NYC)"]
        KE["Kafka Events"]
        PG["Postgres CDC"]
    end

    subgraph PROCESS["⚙️ Processing Layer"]
        SB["Spark Batch (local[*])"]
        SS["Spark Streaming"]
        CB["CDC Bridge (cdc_bridge)"]
    end

    subgraph STORE["💾 Storage Layer (MinIO S3)"]
        R["nyc-raw (153MB)"]
        S["nyc-silver (265MB)"]
        Q["nyc-quarantine (36MB)"]
        L["nyc-lookup (12KB)"]
    end

    subgraph CATALOG["🗄️ Catalog & SQL Layer"]
        T["Trino (Hive)"]
        D["dbt-trino (15 models, 9 tests)"]
        SUP["Apache Superset<br/>(7 datasets, 4 charts, dashboard)"]
    end

    subgraph ORCH["🎯 Orchestration Layer"]
        AF["Apache Airflow (3 DAGs)<br/>• e2e_pipeline (@monthly, 7 tasks)<br/>• cdc_pipeline (manual, 3 tasks)<br/>• analytics_refresh (@weekly, 4 tasks)"]
    end

    subgraph DEPLOY["🚀 Deployment Layer"]
        SK["Skaffold ⭐<br/>(build+sync+forward)"]
        HL["Helm Chart (30+ tmpl)"]
        KD["kind (3 nodes)"]
        AF2["Airflow 2.10.5<br/>(KubernetesPodOperator)"]
    end

    subgraph METRICS["📊 Metrics & Results"]
        M1["Valid trips: 10.2M"]
        M2["Invalid trips: 1.07M"]
        M3["dbt tests: 24/24 PASS"]
        M4["Analytics: 10/10 PASS"]
        M5["CDC: ~445 ev/s"]
        M6["Spark: ~9 min (3 months)"]
    end

    RP & KE & PG --> PROCESS
    SB & SS & CB --> STORE
    R & S & Q & L --> CATALOG
    T --> D --> SUP
    ORCH -.->|schedules| PROCESS
    ORCH -.->|schedules| CATALOG
    SK --> HL --> KD
    AF2 --> ORCH
```

---

## 13.2 Service Dependency Graph (Kubernetes)

```mermaid
graph TB
    ZK["Zookeeper<br/>svc-zookeeper:2181"]
    KAFKA["Kafka<br/>svc-kafka:9092"]
    KAFKA_UI["Kafka UI<br/>svc-kafka-ui:8080"]
    SS["Spark Stream<br/>svc-spark-master:7077"]
    DZ["Debezium<br/>svc-debezium:8083"]
    MINIO["MinIO S3<br/>svc-minio:9000<br/>nyc-raw/silver/quarantine/lookup/gold"]
    TRINO["Trino<br/>svc-trino:8080"]
    PG["Postgres CDC<br/>svc-postgres-cdc:5432"]
    DBT["dbt-trino<br/>(Airflow KPO)"]
    SUP["Superset<br/>svc-superset:8088"]
    SB["Spark Batch<br/>(Airflow KPO)"]
    AF["Airflow<br/>svc-airflow-webserver:8080<br/>scheduler + webserver"]

    KAFKA --> ZK
    KAFKA_UI --> KAFKA
    SS --> KAFKA
    DZ --> KAFKA
    DZ --> PG
    TRINO --> MINIO
    SUP --> TRINO
    DBT --> TRINO
    SB --> MINIO
    SS --> MINIO
    AF -->|KubernetesPodOperator| SB
    AF -->|KubernetesPodOperator| SS
    AF --> TRINO
    AF --> DBT
    AF --> SUP
```

---

## 13.3 Key Design Decisions

### Tại sao dùng file-based Hive metastore?
- **Không cần Hive service riêng** — Giảm độ phức tạp
- **Hạn chế**: Không support `RENAME TABLE` → dbt models phải là `view`
- **Phù hợp**: Dev/testing pipeline, không phải production lớn

### Tại sao dùng MinIO thay vì HDFS?
- **S3-compatible** — Có thể chuyển lên AWS S3 mà không đổi code
- **Nhẹ** — Chạy trong container, không cần cluster riêng
- **Hạn chế**: Không support atomic rename → cần S3 commit fix

### Tại sao dùng Skaffold vs Makefile?
| Tiêu chí | Skaffold | Makefile |
|----------|----------|----------|
| Auto-rebuild | ✅ | ❌ |
| File sync | ✅ | Manual |
| Port-forward | ✅ | Manual |
| Watch mode | ✅ | ❌ |
| Đơn giản | Trung bình | Cao |

Skaffold là lựa chọn **chính thức và duy nhất** cho triển khai. Makefile/Docker Compose chỉ dùng cho debug.

### Tại sao dùng Airflow (K8s) làm orchestrator?
- **KubernetesPodOperator** — mỗi task là một pod riêng biệt, tự động cleanup
- **Tự động theo lịch**: @monthly, @weekly — không cần can thiệp thủ công
- **Backfill**: `catchup=True` cho phép xử lý dữ liệu quá khứ
- **Logging**: Logs pod trực tiếp trên Airflow UI (get_logs=True)

---

## 13.4 Lưu ý quan trọng (Best Practices)

### K8s Service Names
```
Luôn dùng prefix svc-: svc-kafka, svc-minio, svc-trino...
Không dùng: kafka, minio, trino...
```

### Spark S3A Packages
```
--packages org.apache.hadoop:hadoop-aws:3.3.4,...
--conf spark.jars.ivy=/opt/project/.ivy2
--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2
```

### dbt Models
```
Tất cả models: materialized='view'
Không dùng materialized='table' (Hive không support RENAME TABLE)
```

### Output Mode
```
Spark: luôn dùng mode("append")
Không dùng mode("overwrite") với MinIO
```

### CDC Bridge
```
Async mode (default): ~500 ev/s
Sync mode (--sync): ~9 ev/s (~50x slower)
```

### Skaffold Dev
```
skaffold dev --namespace nyc-taxi
→ Auto build + deploy + sync + port-forward + watch
```

---

## 13.5 Troubleshooting Checklist (Skaffold/K8s)

| Vấn đề | Nguyên nhân | Giải pháp |
|--------|-------------|-----------|
| **Skaffold build fails** | Dockerfile lỗi | Kiểm tra syntax, `skaffold build --namespace nyc-taxi` để debug |
| **Helm deploy fails** | Namespace stuck | Gỡ finalizers (xem mục 2.5) |
| **Spark S3A fails** | Missing --packages | Dùng `--packages` trên CLI, không phải `spark.jars.packages` |
| **dbt build fails** | Hive RENAME TABLE | Set `materialized='view'` |
| **Trino không thấy partitions** | Metadata chưa sync | `CALL sync_partition_metadata(...)` |
| **Kafka connection fails** | Wrong service name | Dùng `svc-kafka:9092` (K8s) |
| **Namespace stuck** | Finalizers | `kubectl replace --raw /api/v1/namespaces/nyc-taxi/finalize -f ...` |
| **Ivy cache fails** | Permissions | `chmod -R 777 /opt/project/.ivy2/` |
| **Port-forward dies** | Skaffold không chạy | Dùng `skaffold dev` hoặc `./scripts/k8s_ui.sh start` |
| **File-sync không hoạt động** | Skaffold watch tắt | Chạy `skaffold dev` hoặc sync thủ công (xem mục 2.5) |

---

## 13.6 Tài liệu tham khảo

| File | Nội dung |
|------|----------|
| `docs/01-tong-quan-kien-truc.md` | Tổng quan kiến trúc |
| `docs/02-huong-dan-trien-khai.md` | Hướng dẫn triển khai |
| `docs/03-spark-processing.md` | Xử lý Spark batch + streaming |
| `docs/04-dbt-models.md` | dbt models và transformations |
| `docs/05-trino-catalog.md` | Trino catalog và Hive metadata |
| `docs/06-airflow-dags.md` | Airflow DAGs orchestration |
| `docs/07-cdc-pipeline.md` | CDC pipeline (Debezium) |
| `docs/08-superset-visualization.md` | Superset dashboard |
| `docs/09-docker-images.md` | Docker images và entrypoints |
| `docs/10-helm-skaffold.md` | Helm chart và Skaffold |
| `docs/11-scripts-utilities.md` | Scripts và tiện ích |
| `docs/12-data-flow-storage.md` | Luồng dữ liệu và storage |
| `docs/13-architecture-summary.md` | Tổng kết kiến trúc |



# 14. Biểu Đồ Kiến Trúc Skaffold, Helm và Kubernetes

## 14.1 Skaffold Dev Workflow

```mermaid
flowchart TD
    subgraph USER["👤 Developer"]
        CMD["skaffold dev --namespace nyc-taxi"]
    end

    subgraph SKAFFOLD["Skaffold Engine"]
        direction TB
        B["1. Build Artifacts<br/>• nyc-pipeline-tools (Dockerfile)<br/>• nyc-dbt (Dockerfile)<br/>• nyc-airflow (Dockerfile)"]
        PRE["2. Pre-Deploy Hooks<br/>• Xoá immutable jobs<br/>• Sync files → kind-worker PVC"]
        H["3. Helm Deploy<br/>• helm install nyc-taxi chart<br/>• Tạo namespace + resources"]
        POST["4. Post-Deploy Hooks<br/>• Sleep 5s<br/>• In UIs URLs<br/>• Mở browser tabs"]
        PF["5. Port-Forward<br/>• 8 services → localhost:39080-39087<br/>• Skaffold giữ kết nối alive"]
        W["6. Watch Mode<br/>• File changes → sync to PVC<br/>• Dockerfile changes → rebuild + redeploy"]
        
        CMD --> B --> PRE --> H --> POST --> PF --> W
        W -. "loop" .-> B
    end

    subgraph FILESYNC["📁 File Sync Rules"]
        direction LR
        SRC1["airflow/dags/**/*.py"] --> DEST1["/opt/project/airflow/dags/"]
        SRC2["jobs/**/*"] --> DEST2["/opt/project/jobs/"]
        SRC3["scripts/**/*"] --> DEST3["/opt/project/scripts/"]
        SRC4["dbt/**/*"] --> DEST4["/opt/project/dbt/"]
        SRC5["charts/**/*"] --> DEST5["/opt/project/charts/"]
    end

    W --> FILESYNC

    style CMD fill:#4CAF50,color:#fff
    style SKAFFOLD fill:#1565C0,color:#fff
    style FILESYNC fill:#FF8F00,color:#fff
```

---

## 14.2 kind Cluster Topology

```mermaid
graph TB
    subgraph HOST["🖥️ Host Machine (docker)"]
        NP["kind NodePorts<br/>38080-38088, 39000"]
        PF["Skaffold Port-Forwards<br/>39080-39087"]
    end

    subgraph CP["Control-Plane Node"]
        direction TB
        CP1["kube-apiserver"]
        CP2["etcd"]
        CP3["kube-scheduler / controller-manager"]
    end

    subgraph W1["Worker Node 1 (kind-worker)"]
        direction TB
        W1_LBL["Labels: node-type=worker<br/>kubernetes.io/hostname=kind-worker"]
        W1_PV1["PVC: project-files-pv<br/>HostPath: /mnt/nyc-project"]
        W1_PV2["PVC: raw-data-pv<br/>HostPath: /mnt/nyc-data"]
        W1_PODS["Pods:<br/>• file-sync<br/>• minio-setup (Job)<br/>• postgres-init (Job)<br/>• topic-init (Job)<br/>• tất cả services<br/>(nodeSelector: kind-worker)"]
    end

    subgraph W2["Worker Node 2 (kind-worker)"]
        direction TB
        W2_LBL["Labels: node-type=worker"]
        W2_PV1["HostPath mounts available:<br/>/mnt/nyc-project<br/>/mnt/nyc-data"]
        W2_PODS["Pods (nếu cần thêm)"]
    end

    HOST -->|"extraPortMappings"| CP
    HOST -->|"NodePort 38080-38088"| W1
    HOST --> W2

    CP -->|"schedules pods"| W1
    CP -->|"schedules pods"| W2

    W1_PV1 -.->|"hostPath"| HOST
    W1_PV2 -.->|"hostPath"| HOST

    style HOST fill:#E8EAF6,color:#000
    style CP fill:#FFF3E0,color:#000
    style W1 fill:#E3F2FD,color:#000
    style W2 fill:#F3E5F5,color:#000
```

### NodePort Mappings

```mermaid
flowchart LR
    subgraph HOST_PORTS["Host Ports"]
        HP38080["38080"]
        HP38081["38081"]
        HP38082["38082"]
        HP38088["38088"]
        HP39000["39000"]
    end

    subgraph KIND_NODEPORT["kind NodePort (Control-Plane)"]
        NP30080["30080 → Superset"]
        NP30081["30081 → MinIO"]
        NP30082["30082 → Spark"]
        NP30088["30088 → Airflow"]
        NP39000["39000 → Trino"]
    end

    subgraph SKAFFOLD_PF["Skaffold Port-Forward (39080+)"]
        PF80["39080 → svc-superset:8088"]
        PF81["39081 → svc-minio:9000 (API)"]
        PF82["39082 → svc-kafka-ui:8080"]
        PF83["39083 → svc-spark-master:8081"]
        PF84["39084 → svc-trino:8080"]
        PF85["39085 → svc-airflow-webserver:8080"]
        PF86["39086 → svc-minio:9001 (Console)"]
        PF87["39087 → svc-postgres-cdc:5432"]
    end

    HP38080 --> NP30080
    HP38081 --> NP30081
    HP38082 --> NP30082
    HP38088 --> NP30088
    HP39000 --> NP39000

    HP38080 -.->|"alternative"| PF80
    HP38081 -.->|"alternative"| PF81
    HP38082 -.->|"alternative"| PF82
    HP38088 -.->|"alternative"| PF85
```

---

## 14.3 Helm Chart Resource Tree

```mermaid
graph TB
    subgraph CHART["📦 Helm Chart nyc-taxi (0.1.0)"]
        direction TB
        CHART_META["Chart.yaml<br/>apiVersion: v2<br/>type: application<br/>appVersion: 1.0.0"]
        
        subgraph NS["Namespace"]
            N1["namespace.yaml<br/>→ nyc-taxi namespace<br/>Helm managed labels"]
        end

        subgraph STORAGE["Storage Layer"]
            S1["project-files-pv.yaml<br/>PV: 5Gi hostPath<br/>PVC: project-files-pvc<br/>NodeAffinity: kind-worker"]
            S2["raw-data-pv.yaml<br/>PV: hostPath<br/>PVC: raw-data-pvc"]
        end

        subgraph MESSAGING["Messaging Layer"]
            M1["zookeeper/statefulset.yaml"]
            M2["zookeeper/service.yaml<br/>→ svc-zookeeper:2181"]
            M3["kafka/statefulset.yaml<br/>initContainer: wait-zookeeper"]
            M4["kafka/service.yaml<br/>→ svc-kafka:9092"]
            M5["kafka-ui/deployment.yaml"]
            M6["kafka-ui/service.yaml<br/>→ svc-kafka-ui:8080"]
        end

        subgraph STORAGE_SVC["Storage Services"]
            ST1["minio/deployment.yaml<br/>args: server /data --console-address :9001"]
            ST2["minio/service.yaml<br/>→ svc-minio:9000 (API)<br/>→ svc-minio:9001 (Console)"]
            ST3["minio/pvc.yaml<br/>→ minio-data"]
        end

        subgraph PROCESSING["Processing Layer"]
            P1["spark/master-deployment.yaml"]
            P2["spark/master-service.yaml<br/>→ svc-spark-master:7077,8081"]
            P3["spark/worker-deployment.yaml"]
            P4["spark/worker-service.yaml<br/>→ svc-spark-worker:8082"]
        end

        subgraph CDC["CDC Layer"]
            C1["postgres-cdc/statefulset.yaml<br/>wal_level=logical"]
            C2["postgres-cdc/service.yaml<br/>→ svc-postgres-cdc:5432"]
            C3["postgres-cdc/pvc.yaml"]
            C4["debezium/deployment.yaml<br/>Kafka Connect 2.5"]
            C5["debezium/service.yaml<br/>→ svc-debezium:8083"]
        end

        subgraph SQL["SQL & Analytics Layer"]
            Q1["trino/configmap.yaml<br/>hive.properties + config.properties"]
            Q2["trino/deployment.yaml<br/>Trino 435"]
            Q3["trino/service.yaml<br/>→ svc-trino:8080"]
            Q4["superset/configmap.yaml<br/>superset_config.py"]
            Q5["superset/deployment.yaml<br/>Superset 4.0.0"]
            Q6["superset/service.yaml<br/>→ svc-superset:8088"]
        end

        subgraph AIRFLOW["Orchestration Layer"]
            A1["airflow/postgres/statefulset.yaml<br/>Airflow metadata DB"]
            A2["airflow/postgres/service.yaml"]
            A3["airflow/postgres/pvc.yaml"]
            A4["airflow/rbac.yaml<br/>ServiceAccount: airflow-sa<br/>Role + RoleBinding"]
            A5["airflow/init-job.yaml<br/>db migrate + create admin"]
            A6["airflow/file-sync.yaml<br/>sleep infinity pod for skaffold sync"]
            A7["airflow/scheduler/deployment.yaml"]
            A8["airflow/webserver/deployment.yaml<br/>→ svc-airflow-webserver:8080"]
            A9["airflow/webserver/service.yaml"]
        end

        subgraph JOBS["One-Shot Jobs"]
            J1["jobs/minio-setup.yaml<br/>→ mc alias + buckets + upload"]
            J2["jobs/postgres-init.yaml<br/>→ psycopg2 create trips table"]
            J3["jobs/topic-init.yaml<br/>→ wait-kafka + create topics"]
        end

        subgraph DBT["(dbt runs via Airflow KPO)"]
            D1["dbt/ (empty templates)"]
        end
    end

    style CHART fill:#1A237E,color:#fff
    style NS fill:#E8EAF6,color:#000
    style STORAGE fill:#E3F2FD,color:#000
    style MESSAGING fill:#F3E5F5,color:#000
    style STORAGE_SVC fill:#E0F2F1,color:#000
    style PROCESSING fill:#FFF3E0,color:#000
    style CDC fill:#FCE4EC,color:#000
    style SQL fill:#E8F5E9,color:#000
    style AIRFLOW fill:#FFF8E1,color:#000
    style JOBS fill:#F3E5F5,color:#000
    style DBT fill:#ECEFF1,color:#000
```

---

## 14.4 PVC File-Sync Data Flow

```mermaid
flowchart TD
    subgraph DEV_MACHINE["💻 Developer Machine"]
        direction TB
        LOCAL_FILES["Local Project Files<br/>• airflow/dags/<br/>• jobs/<br/>• scripts/<br/>• dbt/<br/>• charts/"]
        SKAFFOLD_WATCH["Skaffold Watch<br/>detects file changes"]
        TAR_CMD["tar cf - | docker exec -i kind-worker ..."]
    end

    subgraph PRE_HOOK["Skaffold Pre-Deploy Hook"]
        direction TB
        HOOK_SYNC["bash -c:<br/>docker exec kind-worker mkdir -p /mnt/nyc-project<br/>tar cf - ... | docker exec -i kind-worker tar xf - -C /mnt/nyc-project"]
        HOOK_DELETE["kubectl delete job -n nyc-taxi --all<br/>Xoá immutable jobs trước Helm deploy"]
    end

    subgraph KIND_WORKER["kind-worker Node"]
        direction TB
        HOSTPATH["/mnt/nyc-project/ (HostPath)"]
        PVC["project-files-pvc<br/>(PersistentVolumeClaim)"]
        SYNC_POD["file-sync Pod<br/>image: nyc-pipeline-tools:k8s<br/>command: sleep infinity<br/>mount: /opt/project"]
        OTHER_PODS["Tất cả Pods dùng PVC:<br/>• Airflow (scheduler, webserver)<br/>• Spark (master, worker)<br/>• Trino<br/>• Superset<br/>• dbt<br/>• CDC jobs<br/>mount: /opt/project"]
    end

    subgraph SKAFFOLD_SYNC["🔄 Skaffold Sync (Hot-Reload)"]
        direction LR
        SYNC_RULES["Sync Rules:<br/>airflow/dags/**/*.py → /opt/project/airflow/dags/<br/>jobs/**/* → /opt/project/jobs/<br/>scripts/**/* → /opt/project/scripts/<br/>dbt/**/* → /opt/project/dbt/<br/>charts/**/* → /opt/project/charts/"]
        SKAFFOLD_PUSH["Skaffold pushes changed files<br/>directly to file-sync pod"]
    end

    LOCAL_FILES -->|"Lần đầu (pre-deploy hook)"| TAR_CMD
    TAR_CMD -->|"docker exec"| HOOK_SYNC
    HOOK_SYNC -->|"ghi vào"| HOSTPATH
    HOSTPATH -->|"hostPath mount"| PVC
    PVC -->|"mounted at /opt/project"| SYNC_POD
    PVC -->|"mounted at /opt/project"| OTHER_PODS

    LOCAL_FILES -->|"Thay đổi file"| SKAFFOLD_WATCH
    SKAFFOLD_WATCH -->|"kích hoạt"| SKAFFOLD_SYNC
    SKAFFOLD_SYNC -->|"push file đến"| SYNC_POD
    SYNC_POD -->|"ghi vào"| PVC
    PVC -->|"tất cả pods thấy<br/>thay đổi ngay lập tức"| OTHER_PODS

    style DEV_MACHINE fill:#E8EAF6,color:#000
    style PRE_HOOK fill:#FFF3E0,color:#000
    style KIND_WORKER fill:#E3F2FD,color:#000
    style SKAFFOLD_SYNC fill:#F3E5F5,color:#000
    style SYNC_POD fill:#4CAF50,color:#fff
    style PVC fill:#FF8F00,color:#fff
```

---

## 14.5 Skaffold Deploy Hook Flow (Chi tiết)

```mermaid
sequenceDiagram
    participant DEV as Developer
    participant SK as Skaffold
    participant DOCK as Docker
    participant K8S as Kubernetes (kind)
    participant PVC as kind-worker PVC
    
    DEV->>SK: skaffold dev --namespace nyc-taxi
    
    Note over SK: === BUILD PHASE ===
    SK->>DOCK: Build nyc-pipeline-tools:k8s
    SK->>DOCK: Build nyc-dbt:k8s
    SK->>DOCK: Build nyc-airflow:k8s
    DOCK-->>SK: Images built
    
    Note over SK: === PRE-DEPLOY HOOK ===
    SK->>K8S: kubectl delete job -n nyc-taxi --all
    K8S-->>SK: Jobs deleted
    
    SK->>K8S: docker exec kind-worker mkdir -p /mnt/nyc-project
    SK->>K8S: tar cf - | docker exec -i kind-worker tar xf - -C /mnt/nyc-project
    Note right of K8S: Excludes: dbt/logs, dbt/target,<br/>.git, __pycache__, *.pyc
    K8S-->>SK: Files synced to PVC
    
    Note over SK: === HELM DEPLOY ===
    SK->>K8S: helm install nyc-taxi charts/nyc-taxi/ --namespace nyc-taxi --create-namespace
    K8S-->>SK: All 30+ resources created
    
    Note over SK: === POST-DEPLOY HOOK ===
    SK->>SK: sleep 5
    SK->>DEV: 🚀 PIPELINE UIs
    SK->>DEV: Superset http://localhost:39080
    SK->>DEV: MinIO API http://localhost:39081
    SK->>DEV: Kafka UI http://localhost:39082
    SK->>DEV: Spark http://localhost:39083
    SK->>DEV: Trino http://localhost:39084
    SK->>DEV: Airflow http://localhost:39085
    SK->>DEV: MinIO Admin http://localhost:39086
    SK->>DEV: Postgres CDC localhost:39087
    
    SK->>DEV: xdg-open http://localhost:39085 (Airflow)
    SK->>DEV: xdg-open http://localhost:39080 (Superset)
    SK->>DEV: xdg-open http://localhost:39082 (Kafka UI)
    
    Note over SK: === PORT-FORWARD ===
    SK->>K8S: port-forward svc-superset 39080:8088
    SK->>K8S: port-forward svc-minio 39081:9000 & 39086:9001
    SK->>K8S: port-forward svc-kafka-ui 39082:8080
    SK->>K8S: port-forward svc-spark-master 39083:8081
    SK->>K8S: port-forward svc-trino 39084:8080
    SK->>K8S: port-forward svc-airflow-webserver 39085:8080
    SK->>K8S: port-forward svc-postgres-cdc 39087:5432
    
    Note over SK,DEV: === WATCH MODE ===
    loop File changes detected
        DEV->>DEV: Edit file in airflow/dags/
        SK->>PVC: Sync changed file to file-sync pod
        PVC-->>K8S: All pods see new file instantly
    end
    
    loop Dockerfile changes
        DEV->>DEV: Edit docker/tools.Dockerfile
        SK->>DOCK: Rebuild nyc-pipeline-tools
        SK->>K8S: Re-deploy chart
    end
```

---

## 14.6 Service Topology & Dependencies

```mermaid
graph TB
    subgraph EXTERNAL["🌐 External Access (Port-Forward 39080-39087)"]
        SUP_UI["Superset UI<br/>:39080"]
        MINIO_UI["MinIO Console<br/>:39086"]
        KAFKA_UI["Kafka UI<br/>:39082"]
        SPARK_UI["Spark Master<br/>:39083"]
        TRINO_UI["Trino<br/>:39084"]
        AIR_UI["Airflow UI<br/>:39085"]
    end

    subgraph K8S_SERVICES["Kubernetes Services (ClusterIP)"]
        direction TB
        SVC_ZK["svc-zookeeper:2181"]
        SVC_KAFKA["svc-kafka:9092"]
        SVC_KAFKA_UI["svc-kafka-ui:8080"]
        SVC_MINIO["svc-minio:9000 / 9001"]
        SVC_SPARK_M["svc-spark-master:7077 / 8081"]
        SVC_SPARK_W["svc-spark-worker:8082"]
        SVC_TRINO["svc-trino:8080"]
        SVC_SUP["svc-superset:8088"]
        SVC_AIR_WEB["svc-airflow-webserver:8080"]
        SVC_PG_CDC["svc-postgres-cdc:5432"]
        SVC_DEBEZIUM["svc-debezium:8083"]
    end

    subgraph PODS["Pods (Containers)"]
        ZK["Zookeeper<br/>confluentinc/cp-zookeeper:7.6.1"]
        KAFKA["Kafka Broker<br/>confluentinc/cp-kafka:7.6.1<br/>initContainer: wait-zookeeper"]
        KAFKA_UI_POD["Kafka UI<br/>provectuslabs/kafka-ui:latest"]
        MINIO["MinIO S3<br/>minio/minio:latest<br/>server /data --console-address :9001"]
        SPARK_M["Spark Master<br/>apache/spark:3.5.1<br/>spark-class Master"]
        SPARK_W["Spark Worker<br/>apache/spark:3.5.1<br/>spark-class Worker"]
        TRINO["Trino Coordinator<br/>trinodb/trino:435"]
        SUP["Superset<br/>apache/superset:4.0.0"]
        AIR_PG["Airflow Postgres<br/>postgres:16-alpine"]
        AIR_SCH["Airflow Scheduler<br/>nyc-airflow:k8s"]
        AIR_WEB["Airflow Webserver<br/>nyc-airflow:k8s"]
        PG_CDC["Postgres CDC<br/>postgres:16-alpine<br/>wal_level=logical"]
        DEBEZIUM["Debezium Connect<br/>debezium/connect:2.5"]
        FILE_SYNC["file-sync<br/>nyc-pipeline-tools:k8s<br/>sleep infinity"]
    end

    subgraph JOBS_PODS["Jobs (One-Shot)"]
        MINIO_SETUP["minio-setup<br/>minio/mc:latest<br/>Create buckets + upload"]
        PG_INIT["postgres-init<br/>nyc-pipeline-tools:k8s<br/>psycopg2 create table"]
        TOPIC_INIT["topic-init<br/>nyc-pipeline-tools:k8s<br/>wait-kafka + create topics"]
    end

    %% Dependencies
    KAFKA -->|"depends on"| SVC_ZK
    KAFKA_UI_POD -->|"connects to"| SVC_KAFKA
    SPARK_W -->|"registers with"| SVC_SPARK_M
    
    TRINO -->|"reads from"| SVC_MINIO
    SUP -->|"queries"| SVC_TRINO
    AIR_SCH -->|"schedules pods on"| SVC_KAFKA
    AIR_SCH --> SVC_TRINO
    AIR_SCH --> SVC_MINIO
    
    DEBEZIUM -->|"connects to"| SVC_KAFKA
    DEBEZIUM -->|"captures from"| SVC_PG_CDC
    
    MINIO_SETUP --> SVC_MINIO
    PG_INIT --> SVC_PG_CDC
    TOPIC_INIT --> SVC_KAFKA

    %% UI connections
    SUP_UI --> SVC_SUP
    MINIO_UI --> SVC_MINIO
    KAFKA_UI --> SVC_KAFKA_UI
    SPARK_UI --> SVC_SPARK_M
    TRINO_UI --> SVC_TRINO
    AIR_UI --> SVC_AIR_WEB

    style EXTERNAL fill:#E8EAF6,color:#000
    style K8S_SERVICES fill:#FFF3E0,color:#000
    style PODS fill:#E3F2FD,color:#000
    style JOBS_PODS fill:#F3E5F5,color:#000
```

---

## 14.7 PVC Mounts và Node Affinity

```mermaid
graph TB
    subgraph KIND_CLUSTER["kind Cluster"]
        subgraph CP["Control-Plane"]
            CP_NODE["kind-control-plane"]
        end

        subgraph W1["Worker 1: kind-worker"]
            W1_AFFINITY["Node Labels:<br/>kubernetes.io/hostname=kind-worker<br/>node-type=worker"]
            W1_MOUNT1["HostPath Mount:<br/>Host: /home/.../nyc_new<br/>→ Container: /mnt/nyc-project"]
            W1_MOUNT2["HostPath Mount:<br/>Host: /home/.../nyc_new/data<br/>→ Container: /mnt/nyc-data"]

            subgraph W1_PV["PersistentVolumes (NodeAffinity: kind-worker)"]
                PV1["project-files-pv<br/>5Gi, hostPath /mnt/nyc-project<br/>ReadWriteOnce"]
                PV2["raw-data-pv<br/>hostPath /mnt/nyc-data<br/>ReadWriteOnce"]
            end

            subgraph W1_PVC["PersistentVolumeClaims"]
                PVC1["project-files-pvc<br/>Requests: 5Gi<br/>StorageClass: ''"]
                PVC2["raw-data-pvc"]
                PVC3["minio-data"]
                PVC4["postgres-cdc-data"]
                PVC5["airflow-postgres-data"]
            end

            subgraph W1_PODS["Pods on kind-worker"]
                FS["file-sync<br/>mount: /opt/project → PVC1"]
                MS["minio-setup (Job)<br/>mount: /data → PVC2"]
                PI["postgres-init (Job)<br/>mount: /opt/project → PVC1"]
                TI["topic-init (Job)<br/>mount: /opt/project → PVC1"]
                MINIO_POD["minio<br/>mount: /data → PVC3"]
                TRINO_POD["trino<br/>mount: /opt/project → PVC1"]
                SUP_POD["superset<br/>mount: /opt/project → PVC1"]
                SPARK_M_POD["spark-master<br/>mount: /opt/project → PVC1"]
                SPARK_W_POD["spark-worker<br/>mount: /opt/project → PVC1"]
                AIR_PODS["airflow-*<br/>mount: /opt/project → PVC1"]
                PG_POD["postgres-cdc<br/>mount: /var/lib/... → PVC4"]
                AIR_PG_POD["airflow-postgres<br/>mount: /var/lib/... → PVC5"]
            end
        end

        subgraph W2["Worker 2: kind-worker2"]
            W2_MOUNT1["HostPath Mount:<br/>Host: /home/.../nyc_new<br/>→ Container: /mnt/nyc-project"]
            W2_MOUNT2["HostPath Mount:<br/>Host: /home/.../nyc_new/data<br/>→ Container: /mnt/nyc-data"]
            W2_AFFINITY["Node Labels:<br/>node-type=worker"]
            W2_SPARE["(Spare capacity -<br/>scheduling only)"]
        end
    end

    PV1 -->|"bound to"| PVC1
    PV2 -->|"bound to"| PVC2
    
    PVC1 -->|"mounted at /opt/project"| FS
    PVC1 --> PI
    PVC1 --> TI
    PVC1 --> TRINO_POD
    PVC1 --> SUP_POD
    PVC1 --> SPARK_M_POD
    PVC1 --> SPARK_W_POD
    PVC1 --> AIR_PODS
    
    PVC2 -->|"mounted at /data"| MS
    
    PVC3 -->|"mounted at /data"| MINIO_POD
    PVC4 -->|"mounted at /var/lib/postgresql/data"| PG_POD
    PVC5 -->|"mounted at /var/lib/postgresql/data"| AIR_PG_POD

    style KIND_CLUSTER fill:#1A237E,color:#fff
    style CP fill:#E8EAF6,color:#000
    style W1 fill:#E3F2FD,color:#000
    style W2 fill:#F3E5F5,color:#000
    style W1_PV fill:#FF8F00,color:#fff
    style W1_PVC fill:#4CAF50,color:#fff
    style W1_PODS fill:#E0F2F1,color:#000
```

---

## 14.8 Port-Forward Mapping

```mermaid
flowchart LR
    subgraph BROWSER["🌐 Browser"]
        SUP["http://localhost:39080<br/>→ Superset (admin/admin)"]
        MINIO_C["http://localhost:39086<br/>→ MinIO Console (minio/minio123)"]
        KAFKA_U["http://localhost:39082<br/>→ Kafka UI"]
        SPARK_U["http://localhost:39083<br/>→ Spark Master"]
        TRINO_U["http://localhost:39084<br/>→ Trino"]
        AIR_U["http://localhost:39085<br/>→ Airflow (admin/admin)"]
    end

    subgraph SKAFFOLD_PF["Skaffold Port-Forward Manager"]
        PF_SUP["svc-superset<br/>8088 → 39080"]
        PF_MINIO_A["svc-minio<br/>9000 → 39081"]
        PF_MINIO_C["svc-minio<br/>9001 → 39086"]
        PF_KAFKA["svc-kafka-ui<br/>8080 → 39082"]
        PF_SPARK["svc-spark-master<br/>8081 → 39083"]
        PF_TRINO["svc-trino<br/>8080 → 39084"]
        PF_AIR["svc-airflow-webserver<br/>8080 → 39085"]
        PF_PG["svc-postgres-cdc<br/>5432 → 39087"]
    end

    subgraph K8S_SERVICES["Kubernetes Services (ClusterIP : namespace nyc-taxi)"]
        SVC_SUP["svc-superset<br/>ClusterIP :8088"]
        SVC_MINIO["svc-minio<br/>ClusterIP :9000, :9001"]
        SVC_KAFKA_UI["svc-kafka-ui<br/>ClusterIP :8080"]
        SVC_SPARK["svc-spark-master<br/>ClusterIP :8081"]
        SVC_TRINO["svc-trino<br/>ClusterIP :8080"]
        SVC_AIR["svc-airflow-webserver<br/>ClusterIP :8080"]
        SVC_PG["svc-postgres-cdc<br/>ClusterIP :5432"]
    end

    SUP --> PF_SUP
    MINIO_C --> PF_MINIO_C
    MINIO_A["MinIO S3 API<br/>http://localhost:39081"] --> PF_MINIO_A
    KAFKA_U --> PF_KAFKA
    SPARK_U --> PF_SPARK
    TRINO_U --> PF_TRINO
    AIR_U --> PF_AIR
    PG_CLI["psql -h localhost -p 39087"] --> PF_PG

    PF_SUP --> SVC_SUP
    PF_MINIO_A --> SVC_MINIO
    PF_MINIO_C --> SVC_MINIO
    PF_KAFKA --> SVC_KAFKA_UI
    PF_SPARK --> SVC_SPARK
    PF_TRINO --> SVC_TRINO
    PF_AIR --> SVC_AIR
    PF_PG --> SVC_PG

    style BROWSER fill:#E8EAF6,color:#000
    style SKAFFOLD_PF fill:#1565C0,color:#fff
    style K8S_SERVICES fill:#FFF3E0,color:#000
```

---

## 14.9 Docker Compose vs Skaffold Deployment Comparison

```mermaid
graph TB
    subgraph COMPOSE["🐳 Docker Compose Mode"]
        direction TB
        COMPOSE_CMD["make infra-up"]
        COMPOSE_BUILD["docker compose build<br/>(implicit)"]
        COMPOSE_START["docker compose up -d<br/>16 services, 6 profiles"]
        COMPOSE_VOL["Docker Volumes<br/>• minio_data<br/>• airflow_postgres_data<br/>• postgres_cdc_data"]
        COMPOSE_BIND["Bind Mount<br/>./ → /opt/project<br/>(hot-reload native)"]
        COMPOSE_NET["Docker Network<br/>nyc_new_default<br/>Service DNS: kafka, minio..."]
        
        COMPOSE_CMD --> COMPOSE_BUILD --> COMPOSE_START
        COMPOSE_START --> COMPOSE_VOL
        COMPOSE_START --> COMPOSE_BIND
        COMPOSE_START --> COMPOSE_NET
    end

    subgraph SKAFFOLD["🚀 Skaffold (Kubernetes) Mode"]
        direction TB
        SKAFFOLD_CMD["skaffold dev --namespace nyc-taxi"]
        
        subgraph BUILD["Build (local push: false)"]
            B1["Build nyc-pipeline-tools<br/>docker/tools.Dockerfile"]
            B2["Build nyc-dbt<br/>docker/dbt.Dockerfile"]
            B3["Build nyc-airflow<br/>docker/airflow.Dockerfile"]
        end
        
        subgraph PRE_HOOK["Pre-Deploy Hooks (host)"]
            H1["kubectl delete job -n nyc-taxi --all"]
            H2["tar + docker exec → kind-worker PVC<br/>Sync: dags/ jobs/ scripts/ dbt/ charts/"]
        end
        
        subgraph HELM["Helm Deploy"]
            DPL["helm install nyc-taxi charts/nyc-taxi/<br/>namespace: nyc-taxi<br/>createNamespace: true"]
            DPL_TMPL["30+ templates rendered:<br/>• Deployments, StatefulSets<br/>• Services, ConfigMaps<br/>• PVCs, PVs, Jobs<br/>• RBAC"]
        end
        
        subgraph POST_HOOK["Post-Deploy Hooks"]
            PO1["Print URLs + credentials"]
            PO2["xdg-open browsers"]
        end
        
        subgraph PF["Port-Forward (auto)"]
            PF1["8 port-forwards<br/>39080-39087"]
        end
        
        subgraph SYNC["Sync Rules (Watch)"]
            S1["Airflow DAGs<br/>Jobs, Scripts<br/>dbt, Charts"]
            S2["→ file-sync pod<br/>→ PVC<br/>→ tất cả pods"]
        end
        
        SKAFFOLD_CMD --> BUILD --> PRE_HOOK --> HELM --> POST_HOOK --> PF --> SYNC
        SYNC -.->|"file changes<br/>loop"| BUILD
    end

    subgraph COMPARE["Comparison"]
        C1["🐳 Compose: Simple, fast, less features"]
        C2["🚀 Skaffold: Production-like, auto-sync, auto-rebuild"]
    end

    style COMPOSE fill:#E8F5E9,color:#000
    style SKAFFOLD fill:#E3F2FD,color:#000
    style COMPARE fill:#FFF8E1,color:#000
```

---

## 14.10 kind Cluster Creation Flow

```mermaid
flowchart TD
    START("make k8s-up / kind create cluster") --> CHECK{Cluster exists?}
    CHECK -->|"No"| CREATE["kind create cluster --config kind.yaml<br/>• 3 nodes: 1 CP + 2 workers<br/>• extraPortMappings: 38080-38088, 39000<br/>• extraMounts on workers"]
    CHECK -->|"Yes"| BUILD_IMAGES
    
    CREATE --> BUILD_IMAGES["make k8s-images<br/>• docker build 3 images<br/>• kind load docker-image"]
    BUILD_IMAGES --> DEPLOY["make k8s-deploy<br/>• kubectl apply ordered manifests<br/>• namespace → storage → services → jobs"]
    DEPLOY --> WAIT["kubectl wait --for=condition=ready pod --all -n nyc-taxi<br/>Timeout: 300s"]
    WAIT --> PORT["make k8s-ui / skaffold port-forward<br/>• 39080-39087<br/>• setsid -f auto-restart"]
    PORT --> DONE["✅ All services running"]

    CREATE --> DETAILS["kind Config Details"]
    
    subgraph DETAILS["kind.yaml Config"]
        CP["Control-Plane Node<br/>• NodePort: 38080→30080 (Superset)<br/>• NodePort: 38081→30081 (MinIO)<br/>• NodePort: 38082→30082 (Spark)<br/>• NodePort: 38088→30088 (Airflow)<br/>• NodePort: 39000→39000 (Trino)"]
        
        W1["Worker 1 (kind-worker)<br/>• hostPath: /mnt/nyc-project<br/>• hostPath: /mnt/nyc-data<br/>• Label: node-type=worker"]
        
        W2["Worker 2 (kind-worker2)<br/>• hostPath: /mnt/nyc-project<br/>• hostPath: /mnt/nyc-data<br/>• Label: node-type=worker"]
    end

    style START fill:#4CAF50,color:#fff
    style CREATE fill:#FF8F00,color:#fff
    style BUILD_IMAGES fill:#1565C0,color:#fff
    style DEPLOY fill:#7B1FA2,color:#fff
    style WAIT fill:#C62828,color:#fff
    style PORT fill:#00838F,color:#fff
    style DONE fill:#2E7D32,color:#fff
    style DETAILS fill:#E8EAF6,color:#000
```



