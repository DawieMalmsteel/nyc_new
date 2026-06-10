# Makefile — NYC Taxi Pipeline
# ============================
# Mặc định chạy trên Kubernetes (kind). Docker Compose vẫn hỗ trợ cho local dev.
#
# Usage:
#   make k8s-start            # Start everything (cluster + services + UIs)
#   make k8s-pipeline         # Run full data pipeline
#   make k8s-stop             # Scale down all services
#   make k8s-destroy          # Delete cluster (volumes + images)
#   make k8s-ui               # Start port-forwards for all UIs

SHELL := /bin/bash

KIND_CLUSTER ?= kind
KIND_CONFIG ?= kind.yaml
DOCKER_NETWORK ?= nyc_new_default

# ──────────────────────────────────────────────
# I. Kubernetes (kind) — Primary workflow
# ──────────────────────────────────────────────
.PHONY: k8s-cluster k8s-images k8s-deploy k8s-start k8s-stop k8s-destroy
.PHONY: k8s-ui k8s-ui-stop k8s-pipeline k8s-status k8s-logs k8s-verify

k8s-cluster:                    ## Create kind cluster (3 nodes)
	kind create cluster --name $(KIND_CLUSTER) --config $(KIND_CONFIG)

k8s-images:                     ## Build & load custom images into kind
	docker build -q -f docker/tools.Dockerfile -t nyc-pipeline-tools:k8s . && \
	docker build -q -f docker/dbt.Dockerfile -t nyc-dbt:k8s . && \
	docker build -q -f docker/airflow.Dockerfile -t nyc-airflow:k8s . && \
	kind load docker-image nyc-pipeline-tools:k8s nyc-dbt:k8s nyc-airflow:k8s \
	  --name $(KIND_CLUSTER)

k8s-deploy:                     ## Deploy all K8s manifests (ordered)
	@echo "=== Deploying K8s manifests ==="
	-kubectl delete job -n nyc-taxi --all 2>/dev/null; kubectl delete job --all 2>/dev/null; true
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
	kubectl apply -f k8s/airflow/scheduler/
	kubectl apply -f k8s/airflow/webserver/
k8s-start:                      ## Start: cluster → images → services → UIs
	@if ! kind get clusters 2>/dev/null | grep -q "^$(KIND_CLUSTER)$$"; then \
		echo "=== Creating kind cluster ==="; \
		kind create cluster --name $(KIND_CLUSTER) --config $(KIND_CONFIG); \
		echo "=== Building & loading images ==="; \
		$(MAKE) k8s-images; \
	fi
	$(MAKE) k8s-deploy
	@echo "=== Scaling up services ==="
	-kubectl scale deployment -n nyc-taxi --all --replicas=1 2>/dev/null || true
	-kubectl scale statefulset -n nyc-taxi --all --replicas=1 2>/dev/null || true
	@echo "=== Waiting for core services ==="
	-kubectl wait --for=condition=ready pod -l app=zookeeper -n nyc-taxi --timeout=120s 2>/dev/null || true
	-kubectl wait --for=condition=ready pod -l app=kafka -n nyc-taxi --timeout=120s 2>/dev/null || true
	-kubectl wait --for=condition=ready pod -l app=minio -n nyc-taxi --timeout=120s 2>/dev/null || true
	-kubectl wait --for=condition=ready pod -l app=trino -n nyc-taxi --timeout=120s 2>/dev/null || true
	-kubectl wait --for=condition=ready pod -l app=superset -n nyc-taxi --timeout=120s 2>/dev/null || true
	$(MAKE) k8s-ui

k8s-stop: k8s-ui-stop          ## Scale down all services (keep data)
	@echo "=== Scaling down ==="
	kubectl scale deployment -n nyc-taxi --all --replicas=0 2>/dev/null || true
	kubectl scale statefulset -n nyc-taxi --all --replicas=0 2>/dev/null || true
	@echo "All services stopped"

k8s-destroy: k8s-ui-stop       ## Delete cluster (services + volumes + images)
	@echo "=== Deleting cluster ==="
	kind delete cluster --name $(KIND_CLUSTER)
	@echo "Cluster deleted"

k8s-ui:                        ## Start port-forwards for all UIs
	@./scripts/k8s_ui.sh start

k8s-ui-stop:                   ## Stop all port-forwards
	@./scripts/k8s_ui.sh stop

k8s-pipeline:                  ## Run full pipeline: init → spark → trino → dbt → bridge
	@echo "=== 1/3 MinIO setup ==="
	kubectl apply -f k8s/jobs/minio-setup.yaml -n nyc-taxi
	kubectl wait --for=condition=complete job/minio-setup -n nyc-taxi --timeout=120s
	@echo "=== 2/10 Init jobs ==="
	kubectl apply -f k8s/jobs/postgres-init.yaml -n nyc-taxi
	kubectl apply -f k8s/jobs/topic-init.yaml -n nyc-taxi
	kubectl wait --for=condition=complete job/postgres-init -n nyc-taxi --timeout=60s
	kubectl wait --for=condition=complete job/topic-init -n nyc-taxi --timeout=60s
	@echo "=== 3/10 CDC seed + register ==="
	kubectl apply -f k8s/jobs/cdc-seed.yaml -n nyc-taxi
	kubectl apply -f k8s/jobs/cdc-register.yaml -n nyc-taxi
	kubectl wait --for=condition=complete job/cdc-seed -n nyc-taxi --timeout=120s
	kubectl wait --for=condition=complete job/cdc-register -n nyc-taxi --timeout=120s
	@echo "=== 4/10 Spark batch (3 months) ==="
	kubectl apply -f k8s/jobs/spark-batch-m01.yaml -n nyc-taxi
	kubectl apply -f k8s/jobs/spark-batch-m02.yaml -n nyc-taxi
	kubectl apply -f k8s/jobs/spark-batch-m03.yaml -n nyc-taxi
	kubectl wait --for=condition=complete job/spark-batch-m01 -n nyc-taxi --timeout=600s
	kubectl wait --for=condition=complete job/spark-batch-m02 -n nyc-taxi --timeout=600s
	kubectl wait --for=condition=complete job/spark-batch-m03 -n nyc-taxi --timeout=600s
	@echo "=== 5/10 Trino bootstrap ==="
	kubectl apply -f k8s/jobs/trino-bootstrap.yaml -n nyc-taxi
	kubectl wait --for=condition=complete job/trino-bootstrap -n nyc-taxi --timeout=120s
	@echo "=== 6/10 dbt build ==="
	kubectl apply -f k8s/dbt/job.yaml -n nyc-taxi
	kubectl wait --for=condition=complete job/dbt-build -n nyc-taxi --timeout=180s
	@echo "=== 7/10 CDC bridge ==="
	kubectl apply -f k8s/jobs/cdc-bridge.yaml -n nyc-taxi
	kubectl wait --for=condition=complete job/cdc-bridge -n nyc-taxi --timeout=120s
	@echo "=== Pipeline complete ==="

k8s-status:                    ## Show pod status
	kubectl get pods -n nyc-taxi -o wide

k8s-logs:                      ## Tail logs (usage: make k8s-logs JOB=spark-batch-m01)
	kubectl logs -n nyc-taxi job/$(JOB) --follow

k8s-verify:                    ## Verify row counts via Trino
	kubectl run -n nyc-taxi --rm -i temp --image=nyc-pipeline-tools:k8s --restart=Never \
	  -- python3 scripts/verify_mart.py

.PHONY: infra-up infra-up-all infra-down infra-status infra-logs
.PHONY: kafka-topics kafka-publish
.PHONY: cdc-up cdc-seed cdc-register cdc-bridge
.PHONY: spark-batch spark-streaming
.PHONY: trino-bootstrap trino-shell
.PHONY: dbt-build dbt-run dbt-test
.PHONY: superset-bootstrap superset-check
.PHONY: airflow-up airflow-trigger
.PHONY: verify-mart verify-analytics verify-cdc verify-all
.PHONY: clean-silver clean-quarantine clean-all

## Infrastructure
infra-up:                      ## Start core services (ZK, Kafka, MinIO, Spark)
	docker compose up -d zookeeper kafka kafka-ui minio spark-master spark-worker

infra-up-all:                  ## Start everything
	docker compose --profile tools --profile trino --profile dbt --profile superset --profile airflow up -d

infra-down:                    ## Stop services (keep volumes)
	docker compose down

infra-status:                  ## Show container status
	docker compose ps

infra-logs:                    ## Tail logs (usage: make infra-logs SVC=trino)
	docker compose logs --tail=50 -f $(SVC)

## Kafka
kafka-topics:                  ## Create topics
	docker compose run --rm topic-init

kafka-publish:                 ## Publish events (default 5000)
	docker compose run --rm \
	  -e TOPIC="$${TOPIC:-taxi.trip.events.$$(date +%s)}" \
	  -e MAX_EVENTS="$${MAX_EVENTS:-5000}" \
	  -e INVALID_RATE="$${INVALID_RATE:-0.02}" \
	  generator

## CDC
cdc-up:                        ## Start Postgres + Debezium
	docker compose --profile tools up -d nyc_postgres debezium

cdc-seed:                      ## Seed Postgres from parquet (5000 rows)
	docker compose --profile tools run --rm cdc-seed

cdc-register:                  ## Register Debezium connector
	docker compose --profile tools run --rm cdc-register

cdc-bridge:                    ## Bridge CDC → events
	docker compose --profile tools run --rm cdc-bridge

## Spark
spark-batch:                   ## Batch backfill via MinIO S3
	docker run --rm \
	  --network "$(DOCKER_NETWORK)" \
	  -v "$(PWD):/opt/project" -w /opt/project \
	  -e HOME=/tmp \
	  --entrypoint /opt/spark/bin/spark-submit \
	  apache/spark:3.5.1 \
	  --master local[*] \
	  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
	  --conf spark.jars.ivy=/tmp/.ivy2 \
	  /opt/project/jobs/spark_local_batch.py \
	  --input "s3a://nyc-raw/yellow_taxi/year=2024/month=$${MONTH:-01}/yellow_tripdata_2024-$${MONTH:-01}.parquet" \
	  --lookup "s3a://nyc-lookup/taxi_zone_lookup.csv"

spark-streaming:               ## Submit streaming job (MinIO S3)
	TOPIC="$${TOPIC:-taxi.trip.events}" bash scripts/start_streaming_job_docker.sh

## Trino
trino-bootstrap:               ## Register tables in Hive catalog
	docker compose --profile tools --profile trino run --rm trino-bootstrap

trino-shell:                   ## Interactive Trino shell
	docker exec -it nyc_trino trino --user analytics

## dbt
dbt-build:                     ## Full dbt build: models + tests
	docker compose --profile tools --profile dbt run --rm dbt dbt build

dbt-run:                       ## Run models only
	docker compose --profile tools --profile dbt run --rm dbt dbt run

dbt-test:                      ## Run tests only
	docker compose --profile tools --profile dbt run --rm dbt dbt test

## Superset
superset-bootstrap:            ## Register DB, charts, dashboard
	docker exec -i nyc_superset python3 < scripts/superset_bootstrap.py

superset-check:                ## List Superset resources
	docker exec -i nyc_superset python3 < scripts/superset_check.py

## Airflow
airflow-up:                    ## Start Airflow (requires infra-up)
	docker compose --profile airflow up -d

airflow-trigger:               ## Trigger DAG (usage: DAG=nyc_analytics_refresh)
	docker exec nyc_airflow_webserver airflow dags trigger $(DAG)

## Verify
verify-mart:                   ## Row counts in Trino
	python3 scripts/verify_mart.py

verify-analytics:              ## 10 SQL questions (PASS 10/10)
	python3 scripts/run_analytics_questions.py

verify-cdc:                    ## Verify CDC pipeline
	@echo "=== Postgres ==="
	@docker compose exec -T nyc_postgres psql -U postgres -d nyc_taxi -c "SELECT count(*) FROM trips;" 2>/dev/null
	@echo "=== Debezium ==="
	@curl -sf http://localhost:8084/connectors/nyc-postgres-connector/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); s=d['connector']['state']; print(f'State: {s}'); sys.exit(0 if s=='RUNNING' else 1)" || echo "Debezium not found"
	@echo "=== CDC topic ==="
	@docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic nyc_cdc.public.trips --time -1 2>/dev/null | cut -d: -f3 | xargs -I{} echo "Messages: {}"

verify-all:                    ## Full pipeline verification
	@echo "=== 1/6 Spark batch ==="
	$(MAKE) spark-batch
	@echo "=== 2/6 Trino bootstrap ==="
	$(MAKE) trino-bootstrap
	@echo "=== 3/6 dbt build ==="
	$(MAKE) dbt-build
	@echo "=== 4/6 Mart verification ==="
	$(MAKE) verify-mart
	@echo "=== 5/6 Analytics ==="
	$(MAKE) verify-analytics
	@echo "=== 6/6 Superset ==="
	$(MAKE) superset-check
	@echo "=== ALL VERIFIED ==="

## Clean
clean-silver:                  ## Delete silver parquet data
	rm -rf data/silver/trips/*

clean-quarantine:              ## Delete quarantine parquet
	rm -rf data/quarantine/invalid_trips/*

clean-all:                     ## Delete all generated data
	rm -rf data/silver/trips/* data/quarantine/invalid_trips/* data/checkpoints/* data/trino-metastore/*
	mkdir -p data/silver/trips data/quarantine/invalid_trips
	@echo "All generated data cleaned"
