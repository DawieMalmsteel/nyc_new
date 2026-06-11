# Pipeline Dữ Liệu Taxi NYC

Pipeline xử lý dữ liệu chuyến đi taxi NYC từ đầu đến cuối — batch và streaming. Hai chế độ triển khai:

- **Kubernetes (kind)** — chính, giống production (3 nodes, tất cả dịch vụ trong pod)
- **Docker Compose** — phát triển local (một máy, nhẹ hơn)

MinIO S3 là tầng lưu trữ, Spark xử lý dữ liệu, Trino/Hive làm catalog, dbt-trino biến đổi dữ liệu, Apache Superset hiển thị dashboard. Trên Kubernetes, **Airflow** là công cụ điều phối chính — pipeline tự động chạy theo lịch sau khi cluster khởi động. Makefile targets chỉ dành cho phát triển local (Docker Compose).

## Kiến trúc

Mọi thứ đều bắt đầu từ **file Parquet thô** tải từ NYC TLC:

1. **`make k8s-pipeline`** tải Parquet thô + CSV lookup zone lên MinIO S3 (`nyc-raw`, `nyc-lookup`)
2. **Spark Batch** đọc từ `s3a://nyc-raw`, enrich + validate, chia thành **hợp lệ** (`nyc-silver/trips/`) và **không hợp lệ** (`nyc-quarantine/`)
3. **Trino Hive catalog** register bảng external trỏ đến đường dẫn MinIO S3
4. **dbt-trino** biến đổi dữ liệu silver thành staging → marts → gold views
5. **Superset** truy vấn Trino để hiển thị biểu đồ và dashboard
6. **Airflow** điều phối toàn bộ luồng

Luồng streaming: **Kafka** events → **Spark Streaming** (cùng logic enrich) → append vào `nyc-silver/trips/`.
Luồng CDC: **Postgres WAL** → **Debezium** → Kafka → **cdc-bridge** → `taxi.trip.events` → Spark Streaming.

```mermaid
flowchart TD
    subgraph SOURCE["Nguồn dữ liệu"]
        RP[("Raw Parquet<br/>NYC TLC")]
        K1[("Kafka<br/>taxi.trip.events")]
        PG[("Postgres WAL")]
    end

    subgraph MINIO["MinIO S3 Storage"]
        RAW[("nyc-raw")]
        SILVER[("nyc-silver<br/>trips/")]
        QUARANTINE[("nyc-quarantine<br/>invalid_trips/")]
        LOOKUP[("nyc-lookup<br/>taxi_zone_lookup.csv")]
    end

    subgraph PROCESS["Xử lý"]
        SB["Spark Batch<br/>local[*]"]
        SS["Spark Streaming"]
        BRIDGE[cdc-bridge]
    end

    RP -->|make minio-setup| RAW
    RAW --> SB
    SB --> SILVER
    SB --> QUARANTINE
    K1 --> SS
    SS --> SILVER
    PG -->|Debezium| BRIDGE --> K1

    SILVER --> TRINO[Trino Hive Catalog]
    LOOKUP --> TRINO
    TRINO --> DBT[dbt-trino<br/>15 views]
    DBT --> SUPERSET[Apache Superset]
    AIRFLOW[Airflow] -..-> SB & TRINO & DBT & SUPERSET
```

### Chế độ triển khai

| Chế độ | Cluster | Dịch vụ | Dữ liệu | Dùng cho |
|--------|---------|---------|---------|----------|
| **Kubernetes (kind)** | 3 nodes (kind) | Pods, Services, PVCs | MinIO S3 | Giống production, đầy đủ tính năng |
| **Docker Compose** | Docker host | Containers qua compose profiles | MinIO S3 | Dev local, debug nhẹ |

## Bắt đầu nhanh — Kubernetes (chính)

```bash
# 1. Khởi động mọi thứ (cluster + images + services + UIs)
make k8s-start

# 2. Kích hoạt DAG qua Airflow UI hoặc CLI:
#    UI: http://localhost:39085 -> admin/admin -> unpause + trigger nyc_e2e_pipeline
#    CLI: make airflow-trigger DAG=nyc_e2e_pipeline (cần port-forward)
#
#    Sau khi cluster khởi động, Airflow tự động chạy pipeline theo lịch
#    (@monthly cho e2e, @weekly cho analytics)

# 3. Kiểm tra analytics (10 câu SQL truy vấn Trino)
make k8s-verify-analytics

# 4. Kiểm tra CDC (Postgres count, Debezium status, Kafka topic)
make k8s-verify-cdc

# 5. Dừng (scale down, giữ dữ liệu)
make k8s-stop

# 6. Xoá (xoá cluster, mất hết dữ liệu)
make k8s-destroy
```

## Bắt đầu nhanh — Docker Compose

```bash
# 1. Khởi động hạ tầng (ZK, Kafka, MinIO, Spark)
make infra-up

# 2. Tạo Kafka topics
make kafka-topics

# 3. Tải dữ liệu thô lên MinIO
make minio-setup

# 4. Chạy Spark batch backfill (3 tháng, ~10.2M dòng)
make spark-batch   # đọc từ s3a://nyc-raw, ghi vào s3a://nyc-silver

# 5. Register bảng trong Trino Hive catalog
make trino-bootstrap

# 6. Build dbt models + chạy test
make dbt-build     # 15 models + 9 tests, kỳ vọng 24/24 PASS

# 7. Kiểm tra dữ liệu
make verify-mart       # Đếm dòng trong Trino
make verify-analytics  # 10 câu SQL, kỳ vọng PASS 10/10

# 8. Khởi động dashboard
make superset-bootstrap  # http://localhost:8088 (admin/admin)

# Toàn bộ pipeline trong một lệnh
make verify-all
```

## Tất cả Makefile Targets

### Kubernetes (kind)
| Target | Mô tả |
|--------|-------|
| `k8s-cluster` | Tạo kind cluster (3 nodes) |
| `k8s-images` | Build + load images vào kind |
| `k8s-deploy` | Triển khai tất cả manifest (có thứ tự) |
| `k8s-start` | Full start: cluster → images → services → UIs |
| `k8s-stop` | Scale down services (giữ dữ liệu) |
| `k8s-destroy` | Xoá cluster (services + volumes + images) |
| `k8s-ui` | Bật port-forwards cho tất cả UIs (39080-39086) |
| `k8s-ui-stop` | Tắt tất cả port-forwards |
| `k8s-pipeline` | Chạy pipeline đầy đủ: init → spark → trino → dbt → bridge → verify |
| `k8s-status` | Xem trạng thái pod |
| `k8s-logs JOB=<tên>` | Xem log của job |
| `k8s-verify` | Kiểm tra row counts qua Trino |
| `k8s-verify-analytics` | Chạy 10 câu SQL analytics |
| `k8s-verify-cdc` | Kiểm tra CDC pipeline (Postgres, Debezium, Kafka) |
| `k8s-clean` | Xoá dữ liệu MinIO + jobs (bắt đầu sạch) |

### Docker Compose
| Target | Mô tả |
|--------|-------|
| `infra-up` | Khởi động core services (ZK, Kafka, MinIO, Spark) |
| `infra-up-all` | Khởi động mọi thứ (gồm Trino, dbt, Superset, Airflow) |
| `infra-down` | Dừng services (giữ volumes) |
| `infra-status` | Xem trạng thái container |
| `infra-logs SVC=<tên>` | Xem log |
| `kafka-topics` | Tạo Kafka topics |
| `cdc-up` | Khởi động Postgres + Debezium |
| `cdc-seed` | Nạp dữ liệu từ Parquet vào Postgres (5000 dòng) |
| `cdc-register` | Đăng ký Debezium connector |
| `cdc-bridge` | Bridge CDC events → format taxi.trip.events |
| `cdc-verify` | Kiểm tra CDC E2E |
| `spark-batch` | Batch backfill qua MinIO S3 |
| `spark-streaming` | Gửi streaming job |
| `trino-bootstrap` | Register bảng trong Hive catalog |
| `trino-shell` | Trino shell tương tác |
| `dbt-build` | Full dbt build: models + tests |
| `dbt-run` | Chạy models chỉ |
| `dbt-test` | Chạy tests chỉ |
| `superset-bootstrap` | Register DB, charts, dashboard |
| `superset-check` | Liệt kê tài nguyên Superset |
| `airflow-up` | Khởi động Airflow |
| `airflow-trigger DAG=<tên>` | Kích hoạt DAG |
| `verify-mart` | Đếm dòng trong Trino |
| `verify-analytics` | 10 câu SQL (PASS 10/10) |
| `verify-cdc` | Kiểm tra CDC pipeline |
| `verify-all` | Kiểm tra toàn bộ pipeline |
| `clean-silver` | Xoá dữ liệu silver parquet |
| `clean-quarantine` | Xoá dữ liệu quarantine |
| `clean-all` | Xoá tất cả dữ liệu đã sinh |

## UIs & Port-forwards

Chế độ Kubernetes dùng `kubectl port-forward` — cổng **39080-39086** (tránh xung đột NodePort 38080 của kind).

| Dịch vụ | URL | Cổng | Thông tin đăng nhập |
|---------|-----|------|-------------------|
| Apache Superset | http://localhost:39080 | 39080 | `admin` / `admin` |
| MinIO API | http://localhost:39081 | 39081 | `minio` / `minio123` |
| Kafka UI | http://localhost:39082 | 39082 | — |
| Spark Master | http://localhost:39083 | 39083 | — |
| Trino | http://localhost:39084 | 39084 | — |
| Airflow | http://localhost:39085 | 39085 | `admin` / `admin` |
| MinIO Console | http://localhost:39086 | 39086 | `minio` / `minio123` |

Chế độ Docker Compose dùng cổng publish trực tiếp (8088, 9000/9001, 8083, v.v.).

## Kết quả Batch

| Chỉ số | Compose | K8s |
|--------|---------|-----|
| Chuyến hợp lệ | 8.480.408 | **10.188.983** |
| Chuyến lỗi | 1.074.370 | **1.074.370** |
| Zone lookup | 265 | 265 |
| dbt tests | 24/24 PASS | 24/24 PASS |
| Analytics | 10/10 PASS | 10/10 PASS |
| CDC bridge | ~2.543 ev/s | ~445 ev/s |
| Spark runtime (3 tháng) | ~10 phút | ~9 phút |

K8s có số liệu cao hơn vì lần chạy sạch gần nhất bao gồm dữ liệu 2002-2024
(nhiều năm hơn lần chạy Docker Compose ban đầu chỉ có 2024).

## Cấu trúc dữ liệu

```
MinIO S3 buckets:
├── nyc-raw/          → yellow_taxi/year=2024/month=01..03/*.parquet
├── nyc-silver/trips/ → pickup_year=*/pickup_month=*/  (10.2M dòng)
├── nyc-quarantine/   → invalid_trips/                  (1.07M dòng)
├── nyc-lookup/       → taxi_zone_lookup.csv            (265 zones)
```

## Thành phần Pipeline

| Tầng | Công nghệ | Vai trò |
|------|-----------|---------|
| Lưu trữ | MinIO S3 | Buckets: `nyc-raw`, `nyc-silver`, `nyc-quarantine`, `nyc-lookup` |
| Xử lý | Spark 3.5.1 | Batch backfill (`spark_local_batch.py`) + Kafka streaming (`spark_stream_taxi_events.py`) |
| Nhắn tin | Kafka + ZK | `taxi.trip.events` (chính), Debezium CDC topics |
| Catalog | Trino 435 | Hive connector + S3 connector, đọc parquet từ MinIO |
| Biến đổi | dbt-trino | 15 views (staging → marts → gold), 9 tests |
| Hiển thị | Apache Superset 4.0.0 | Dashboard kết nối Trino với biểu đồ |
| Điều phối | Airflow 2.10.5 (chính trên K8s) | DAGs: `nyc_e2e_pipeline`, `nyc_analytics_refresh` — Makefile chỉ cho local dev |
| CDC | Debezium 2.5 + Postgres 16 | CDC qua WAL, bridge sang format chuẩn |

## CDC Pipeline

```bash
make cdc-seed       # Nạp dữ liệu từ Parquet vào Postgres (5000 dòng)
make cdc-register   # Đăng ký Debezium connector
make cdc-bridge     # Bridge CDC events → format taxi.trip.events
make cdc-verify     # Kiểm tra CDC E2E
```

CDC bridge chạy vòng lặp poll với idle timeout (5s) — tự động thoát khi không còn event mới.

## Ghi chú phát triển

- **Không cần Python trên host** — tất cả code chạy trong container Docker/K8s.
- **Airflow DAG management**: DAGs `nyc_e2e_pipeline` và `nyc_analytics_refresh` được đồng bộ từ `airflow/dags/` vào Docker image. Trên K8s, Airflow scheduler tự động phát hiện và chạy theo lịch. Kích hoạt thủ công qua UI (http://localhost:39085) hoặc `make airflow-trigger DAG=<tên>`.
- **Kubernetes**: Dùng `make k8s-logs JOB=<tên>` để debug job. Sau khi sửa code, đồng bộ lên PVC:
  `tar cf - scripts/ | docker exec -i kind-worker tar xf - -C /mnt/nyc-project`
- **Spark hybrid** (K8s jobs với Docker Compose infra): `make k8s-pipeline`
  đọc MinIO từ mạng kind trong khi compose services chạy trên host.
- **Spark S3A connector** dùng `--packages hadoop-aws:3.3.4,aws-java-sdk-bundle:1.12.262`
  qua `spark-submit` CLI (không phải `spark.jars.packages`). Ivy cache dùng chung trên PVC.
- **S3 commit fix**: `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2`
  bắt buộc vì MinIO không hỗ trợ atomic S3 rename.
- **MinIO credentials**: `minio` / `minio123`. Spark dùng `s3a://`, Trino dùng `s3://`.
- **Tất cả dbt models** là `materialized='view'` — Hive file-based HMS không hỗ trợ `RENAME TABLE`.
- **Port-forward sống lâu**: `scripts/k8s_ui.sh` dùng `setsid -f` để tiến trình sống sau khi `make` thoát.
- **Kafka bootstrap**: Host `localhost:29092`, container `nyc_kafka:9092`, K8s `svc-kafka:9092`.
