# PLAN.md — NYC Taxi Pipeline (K8s + MinIO S3, cập nhật 2026-06-09)

## 1) Mục tiêu

Pipeline **Kafka-first** cho NYC Taxi, lưu trữ data layer qua **MinIO S3**. Có 2 đường ingestion: **batch** (backfill/verify) và **streaming** (Kafka). Hỗ trợ 2 deployment mode: **Docker Compose** (dev) và **Kubernetes (kind)** (production-like).

```text
[K8s / Docker Compose]

Batch:                     Streaming:
Raw Parquet                Kafka Producer (generator)
    ↓                              ↓
Spark local[*]              Spark Streaming
    ↓                              ↓
    └──→ MinIO S3 (silver / quarantine) ←──┘
                    ↓
              Trino (Hive catalog + S3 connector)
                    ↓
              dbt-trino (15 models + 9 tests)
                    ↓
              Analytics SQL + Superset (7 datasets, dashboard)
                    ↓
              Airflow orchestration
```

---

## 2) Deployment modes

### Docker Compose (dev)
```bash
make infra-up           # Core: ZK, Kafka, MinIO, Spark
make infra-up-all       # All services (Trino, dbt, Superset, Airflow)
make minio-setup        # Upload raw data to MinIO
make spark-batch        # Batch 3 months
make trino-bootstrap    # Register tables from S3
make dbt-build          # dbt models + tests
make verify-all         # Full pipeline verify
```

### Kubernetes / kind (production-like)
```bash
make k8s-cluster        # kind create cluster
make k8s-images         # Build + load images
make k8s-deploy         # Deploy all manifests
make k8s-pipeline       # Run pipeline (jobs in order)
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-superset 39080:8088 &
# See check.md for all port-forwards
```

Xem `check.md` cho danh sách UI URLs + port-forwards đầy đủ.

---

## 3) Trạng thái hiện tại (verified 2026-06-09)

### Storage backend: MinIO S3

| Bucket | Size | Nội dung |
|--------|------|----------|
| `nyc-raw` | 153 MB | Raw parquet files (3 months) |
| `nyc-silver` | 265 MB | Enriched, validated trips (partitioned) |
| `nyc-quarantine` | 36 MB | Invalid trips |
| `nyc-lookup` | 12 KB | Taxi zone lookup CSV |

Spark: `s3a://` protocol. Trino: `s3://` protocol.

### Data source
- `yellow_tripdata_2024-01.parquet` (2,964,624 rows)
- `yellow_tripdata_2024-02.parquet` (3,007,526 rows)
- `yellow_tripdata_2024-03.parquet` (3,582,628 rows)
- `taxi_zone_lookup.csv` (262 zones)
- Tổng: **~9.5M rows**

### Batch pipeline (S3 mode)
- `jobs/spark_local_batch.py` — đọc raw parquet từ **MinIO S3**, enrichment, validation, split valid/invalid
- Chạy `local[*]`, `--packages hadoop-aws:3.3.4,aws-java-sdk-bundle:1.12.262`
- Input `s3a://nyc-raw/...`, output `s3a://nyc-silver/trips` + `s3a://nyc-quarantine/invalid_trips`
- Kết quả 3 tháng (K8s S3 mode, clean run):

  | Month | Valid | Invalid |
  |:--|:--|:--|
  | 01 | 2,724,037 | 240,587 |
  | 02 | 2,719,926 | 287,600 |
  | 03 | 3,036,445 | 546,183 |
  | **Total** | **8,480,375** | **1,074,370** |

### Streaming pipeline (Kafka-first)
- `generator/taxi_event_generator.py` — đọc parquet, push JSON events lên Kafka topic `taxi.trip.events`
- `jobs/spark_stream_taxi_events.py` — Spark Structured Streaming consumer — **always S3 mode**
- CDC bridge optimization: async send + periodic flush, benchmark 2,543 ev/s (280x improvement vs sync)

### Trino (query engine)
- `trinodb/trino:435`, Hive connector + file-based HMS
- S3 config: `hive.s3.endpoint`, `hive.s3.aws-access-key`, `hive.s3.path-style-access=true`
- Schema `hive.nyc` với `trips` (partitioned) + `invalid_trips` + `taxi_zone_lookup`

### dbt (transformation)
- 15 models (3 staging + 8 marts + 4 gold), 9 data tests
- Tất cả `materialized='view'` (HMS không hỗ trợ rename)
- `make dbt-build` → **24/24 PASS**

### Superset (visualization)
- 1 DB (NYC Trino), **7 datasets**, 4 charts, 1 dashboard
- `scripts/superset_bootstrap.py` — idempotent

### Airflow (orchestration)
- 2 DAGs: `nyc_e2e_pipeline`, `nyc_analytics_refresh`
- LocalExecutor, Docker-in-Docker (Docker Compose mode)

---

## 4) Cấu trúc project

```
nyc_new/
├── Makefile                  # Entry point (40+ targets)
├── AGENTS.md / PLAN.md / GUIDE.md / workflow.md / check.md
├── docker-compose.yml        # 16+ services, 6 profiles
│
├── jobs/                     # Spark processors (S3 default)
│   ├── spark_local_batch.py
│   └── spark_stream_taxi_events.py
│
├── scripts/                  # Pipeline utilities
│   ├── superset_bootstrap.py / superset_check.py
│   ├── trino_register.py / trino_sync_partitions.py
│   ├── cdc_bridge.py / cdc_seed.py / cdc_register_connector.py
│   ├── verify_mart.py / run_analytics_questions.py
│   └── start_streaming_job_docker.sh
│
├── k8s/                      # Kubernetes manifests
│   ├── kind.yaml             # 3-node cluster + port mappings
│   ├── namespace/ storage/ jobs/
│   ├── zookeeper/ kafka/ minio/ kafka-ui/
│   ├── spark/ postgres-cdc/ debezium/
│   ├── trino/ superset/
│   ├── airflow/ (postgres, init-job, webserver, scheduler)
│   └── dbt/
│
├── airflow/dags/
│   ├── nyc_e2e_pipeline.py
│   └── nyc_analytics_refresh.py
│
├── dbt/ models/ tests/
├── docker/ Dockerfiles + configs
├── generator/
└── sql/
```

---

## 5) Data Quality rules

- `pickup_ts`, `dropoff_ts` not null
- `dropoff_ts` > `pickup_ts`
- `trip_distance` > 0
- `fare_amount` >= 0
- `total_amount` >= `fare_amount`
- `passenger_count` in [1..6]
- `payment_type` between 1 and 6
- pickup/dropoff_location_id tồn tại trong zone lookup

Invalid → quarantine (không drop im lặng).

---

## 6) Kết quả verify (2026-06-09 — K8s S3 mode, 3 months)

| Bước | Kết quả |
|------|---------|
| spark-batch (S3) 3 tháng | 8,480,375 valid / 1,074,370 invalid |
| trino-bootstrap | `hive.nyc.*` registered |
| dbt-build | **24/24 PASS** |
| superset-bootstrap | 7 datasets, 4 charts, 1 dashboard |
| Airflow DAGs | `nyc_e2e_pipeline`, `nyc_analytics_refresh` unpaused |
| CDC bridge | 2,543 ev/s benchmark |
| UIs (port-forward) | Superset ✅ MinIO ✅ Kafka UI ✅ Spark ✅ Trino ✅ Airflow ✅ |

---

## 7) Còn lại / Next

- [x] MinIO S3 migration (June 2026) — Spark, Trino, dbt đều dùng S3 paths
- [x] K8s deployment (kind cluster) — all services + pipeline verified
- [x] Full data run: 9.5M rows end-to-end via MinIO S3
- [ ] Cloud migration (AWS S3, Glue/EMR, MWAA, QuickSight)
- [ ] Spark streaming K8s job (Kafka consumer)
- [ ] Helm chart cho K8s deployment

---

## 8) Ghi chú quan trọng

- **MinIO S3 default**: Spark dùng `s3a://`, Trino dùng `s3://`. Không cần flag `--s3` hay `S3_MODE=1`.
- **MinIO credentials**: Hardcoded `minio/minio123` — Spark (`fs.s3a.*`), Trino (`hive.s3.*`), và `mc` alias.
- **K8s port-forward**: Ports `38080-38088` bị kind port mapping chiếm → dùng range `39080+` với `--address 0.0.0.0`.
- **dbt `view`**: Hive file-based HMS không hỗ trợ `RENAME TABLE` → tất cả marts là `view`.
- **Spark S3A packages**: Phải dùng `--packages` CLI flag (`spark.jars.packages` trong SparkSession config fail runtime).
- **Trino 435**: `node.environment` + `node.data-dir` trong `node.properties`.
- **Trino auth**: Bỏ `http-server.authentication.type` cho local dev; vẫn yêu cầu `X-Trino-User` header.
- **Superset CSRF**: `WTF_CSRF_ENABLED=False` trong config.
- **Airflow DIND**: Dùng absolute host paths trong Docker CLI (Docker Compose mode).
