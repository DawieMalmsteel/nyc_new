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

## Row Counts (current, S3 mode)

| Table | Rows |
|-------|------|
| `hive.nyc.trips` | 8,480,375 |
| `mart.fact_trips` | 8,480,375 |
| `mart.dim_zone` | 261 |
| `mart.mart_hourly_summary` | 11,745 |
| `mart.mart_revenue_by_day` | 90 |
| `mart.mart_revenue_by_zone` | 24,980 |
| `mart.mart_payment_type_summary` | 6 |

## Storage (MinIO S3)

| Bucket | Size | Description |
|--------|------|-------------|
| `nyc-raw` | 153 MB | Raw parquet files (3 months) |
| `nyc-silver` | 265 MB | Enriched, validated trips |
| `nyc-quarantine` | 36 MB | Invalid trips |
| `nyc-lookup` | 12 KB | Taxi zone lookup CSV |

## Make Targets

### Docker Compose
```bash
make infra-up         # Start core services
make infra-up-all     # Start everything
make spark-batch MONTH=03
make dbt-build
make superset-bootstrap
make minio-setup
```

### K8s
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

## Data Pipeline (S3)

```
Raw Parquet ─upload─► MinIO (nyc-raw)
                          │
                   Spark ─┤ (s3a://)
                          │
                   MinIO (nyc-silver, nyc-quarantine, nyc-lookup)
                          │
                   Trino ─┤ (s3://)
                          │
                   dbt ◄──┘
                          │
                   Superset
```
