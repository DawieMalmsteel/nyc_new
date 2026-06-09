# NYC Taxi Pipeline — Quick Reference

## UIs (port-forward required for K8s)

### K8s mode
| Service | URL | Port-forward |
|---------|-----|-------------|
| Superset | http://localhost:38088 | `kubectl port-forward -n nyc-taxi svc/svc-superset 38088:8080` |
| MinIO | http://localhost:39000 | `kubectl port-forward -n nyc-taxi svc/svc-minio 39000:9000` |
| Kafka UI | http://localhost:38080 | `kubectl port-forward -n nyc-taxi svc/svc-kafka-ui 38080:8080` |
| Spark Master | http://localhost:38082 | `kubectl port-forward -n nyc-taxi svc/svc-spark-master 38082:8081` |
| Trino | http://localhost:38083 | `kubectl port-forward -n nyc-taxi svc/svc-trino 38083:8080` |
| Airflow | http://localhost:38081 | `kubectl port-forward -n nyc-taxi svc/svc-airflow-webserver 38081:8080` |

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
make verify-all       # Full pipeline: batch → Trino → dbt → analytics → Superset
make verify-mart      # Row counts in Trino
make verify-analytics # 10 SQL questions (expect PASS 10/10)
make infra-up         # Start core services (ZK, Kafka, MinIO, Spark)
make infra-up-all     # Start everything
make infra-status     # docker compose ps
make spark-batch MONTH=03
make dbt-build        # Models + tests
make superset-bootstrap
make minio-setup      # Create buckets + upload data
```

## Make Targets (K8s)

```bash
make k8s-cluster      # kind create cluster
make k8s-images       # Build + load images into kind
make k8s-deploy       # Deploy all manifests
make k8s-pipeline     # Run full pipeline (jobs in order)
make k8s-verify       # Verify row counts via Trino
make k8s-status       # kubectl get pods
make k8s-down         # kind delete cluster
```

## Airflow DAGs

| DAG | Description |
|-----|-------------|
| `nyc_e2e_pipeline` | Spark batch → Trino bootstrap → dbt → Superset |
| `nyc_analytics_refresh` | dbt run → Superset → analytics check |

## Data Pipeline

```
Parquet ──► Spark Batch ──► MinIO S3 (silver) ──► Trino ──► dbt ──► Superset
  │                                                     ▲
  ├── Spark Streaming (Kafka) ──► MinIO S3 (silver) ────┘
  └── Debezium CDC (Postgres) ──► Kafka ──► bridge ────┘
```

## Quick start K8s (from scratch)

```bash
# 1. Create cluster
kind create cluster --name kind --config kind.yaml

# 2. Build & load images
docker build -f docker/tools.Dockerfile -t nyc-pipeline-tools:k8s .
docker build -f docker/dbt.Dockerfile -t nyc-dbt:k8s .
docker build -f docker/airflow.Dockerfile -t nyc-airflow:k8s .
kind load docker-image nyc-pipeline-tools:k8s nyc-dbt:k8s nyc-airflow:k8s

# 3. Copy project files to kind-worker hostPath
tar cf - . | docker exec -i kind-worker tar xf - -C /mnt/nyc-project/

# 4. Deploy services
kubectl apply -f k8s/namespace/
kubectl apply -f k8s/storage/
kubectl apply -f k8s/zookeeper/ k8s/kafka/ k8s/minio/ k8s/kafka-ui/
kubectl apply -f k8s/spark/ k8s/postgres-cdc/ k8s/debezium/
kubectl apply -f k8s/trino/ k8s/superset/
kubectl apply -f k8s/airflow/postgres/ k8s/airflow/ k8s/airflow/webserver/ k8s/airflow/scheduler/
kubectl apply -f k8s/dbt/ k8s/jobs/

# 5. Run pipeline
kubectl apply -f k8s/jobs/topic-init.yaml -n nyc-taxi
kubectl apply -f k8s/jobs/postgres-init.yaml -n nyc-taxi
kubectl wait --for=condition=complete job/topic-init -n nyc-taxi --timeout=60s
kubectl wait --for=condition=complete job/postgres-init -n nyc-taxi --timeout=60s
kubectl apply -f k8s/jobs/cdc-seed.yaml -n nyc-taxi
kubectl apply -f k8s/jobs/cdc-register.yaml -n nyc-taxi
kubectl wait --for=condition=complete job/cdc-seed -n nyc-taxi --timeout=120s
kubectl wait --for=condition=complete job/cdc-register -n nyc-taxi --timeout=120s
kubectl apply -f k8s/jobs/spark-batch-m01.yaml -n nyc-taxi
kubectl apply -f k8s/jobs/spark-batch-m02.yaml -n nyc-taxi
kubectl apply -f k8s/jobs/spark-batch-m03.yaml -n nyc-taxi
kubectl wait --for=condition=complete job/spark-batch-m01 -n nyc-taxi --timeout=600s
kubectl wait --for=condition=complete job/spark-batch-m02 -n nyc-taxi --timeout=600s
kubectl wait --for=condition=complete job/spark-batch-m03 -n nyc-taxi --timeout=600s
kubectl apply -f k8s/jobs/trino-bootstrap.yaml -n nyc-taxi
kubectl wait --for=condition=complete job/trino-bootstrap -n nyc-taxi --timeout=120s
kubectl apply -f k8s/dbt/job.yaml -n nyc-taxi
kubectl wait --for=condition=complete job/dbt-build -n nyc-taxi --timeout=180s

# 6. Port-forward UIs
kubectl port-forward -n nyc-taxi svc/svc-superset 38088:8080 &
kubectl port-forward -n nyc-taxi svc/svc-minio 39000:9000 &
kubectl port-forward -n nyc-taxi svc/svc-kafka-ui 38080:8080 &
kubectl port-forward -n nyc-taxi svc/svc-spark-master 38082:8081 &
kubectl port-forward -n nyc-taxi svc/svc-airflow-webserver 38081:8080 &
kubectl port-forward -n nyc-taxi svc/svc-trino 38083:8080 &

# 7. Verify
kubectl run -n nyc-taxi --rm -i temp --image=nyc-pipeline-tools:k8s --restart=Never -- python3 -c "
from trino.dbapi import connect
cur = connect('svc-trino', 8080, user='test').cursor()
cur.execute('SELECT count(*) FROM hive.mart.fact_trips')
print('fact_trips:', cur.fetchone()[0])
"
```
