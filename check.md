# NYC Taxi Pipeline — Quick Reference

## UIs (K8s mode — port-forward required)

Do kind cluster đã map sẵn ports `38080-38088` vào NodePort, dùng range `39080+`:

| Service | URL | Port-forward |
|---------|-----|-------------|
| Superset | http://localhost:39080 | `kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-superset 39080:8088` |
| MinIO (S3 API) | http://localhost:39081 | `kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-minio 39081:9000` |
| MinIO Console | http://localhost:39086 | `kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-minio 39086:9001` |
| Kafka UI | http://localhost:39082 | `kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-kafka-ui 39082:8080` |
| Spark Master | http://localhost:39083 | `kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-spark-master 39083:8081` |
| Trino | http://localhost:39084 | `kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-trino 39084:8080` |
| Airflow | http://localhost:39085 | `kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-airflow-webserver 39085:8080` |

**Credentials:** Superset & Airflow → `admin` / `admin`. MinIO → `minio` / `minio123`.

### Docker Compose mode
| Service | URL | Login |
|---------|-----|-------|
| Superset | http://localhost:8088 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minio` / `minio123` |
| Airflow | http://localhost:8085 | `admin` / `admin` |
| Kafka UI | http://localhost:8080 | — |
| Spark Master | http://localhost:8081 | — |
| Spark Worker | http://localhost:8082 | — |
| Trino (JDBC) | `localhost:8083` | — |

## Row Counts (current)

| Table | Rows |
|-------|------|
| `hive.nyc.trips` | 17,683,612 |
| `mart.fact_trips` | 17,683,612 |
| `mart.dim_zone` | 261 |
| `mart.mart_hourly_summary` | 11,748 |
| `mart.mart_revenue_by_day` | 96 |
| `mart.mart_revenue_by_zone` | 33,334 |
| `mart.mart_payment_type_summary` | 6 |

## Make Targets (Docker Compose)

```bash
make verify-all       # Full pipeline
make infra-up         # Start core services
make infra-up-all     # Start everything
make spark-batch MONTH=03
make dbt-build
make superset-bootstrap
make minio-setup
```

## Make Targets (K8s)

```bash
make k8s-cluster      # kind create cluster
make k8s-images       # Build + load images into kind
make k8s-deploy       # Deploy all manifests
make k8s-pipeline     # Run full pipeline
make k8s-verify       # Verify via Trino
make k8s-status       # kubectl get pods
make k8s-down         # kind delete cluster
```

### Port-forward UIs (K8s)

```bash
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-superset 39080:8088 &
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-minio 39081:9000 &
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-minio 39086:9001 &
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-kafka-ui 39082:8080 &
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-spark-master 39083:8081 &
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-airflow-webserver 39085:8080 &
kubectl port-forward --address 0.0.0.0 -n nyc-taxi svc/svc-trino 39084:8080 &
```

## Data Pipeline

```
Parquet ──► Spark Batch ──► Silver (MinIO S3) ──► Trino ──► dbt ──► Superset
  │                                                    ▲
  ├── Spark Streaming (Kafka) ──► Silver ──────────────┘
  └── Debezium CDC (Postgres) ──► Kafka ──► bridge ────┘
```
