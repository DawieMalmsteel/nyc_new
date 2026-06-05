# Makefile — NYC Taxi Pipeline
# ============================
# Chia nhóm: infra | kafka | spark | trino | dbt | superset | airflow | verify | clean
#
# Profiles Docker:
#   default   → ZK, Kafka, Kafka-UI, MinIO, Spark Master/Worker
#   tools     → topic-init, generator, quality-report, trino-bootstrap
#   trino     → Trino coordinator
#   dbt       → dbt runner
#   superset  → Superset webserver
#   airflow   → Airflow postgres/webserver/scheduler
#
# Usage:
#   make infra-up            # Start core services
#   make infra-up-all        # Start everything
#   make spark-batch         # Batch backfill (fast, no Kafka)
#   make dbt-build           # dbt models + tests
#   make verify-all          # Full pipeline verification

SHELL := /bin/bash

# ──────────────────────────────────────────────
# I. Infrastructure
# ──────────────────────────────────────────────
.PHONY: infra-up infra-up-all infra-down infra-down-all infra-status infra-logs

infra-up:                       ## Start core services (ZK, Kafka, MinIO, Spark)
	docker compose up -d zookeeper kafka kafka-ui minio spark-master spark-worker

infra-up-all:                   ## Start everything (core + Trino + dbt + Superset + Airflow)
	docker compose --profile tools --profile trino --profile dbt --profile superset --profile airflow up -d

infra-down:                     ## Stop services (keep volumes)
	docker compose down

infra-down-all:                 ## Stop services + wipe volumes
	docker compose down -v

infra-status:                   ## Show container status
	docker compose ps

infra-logs:                     ## Tail logs (usage: make infra-logs SVC=trino)
	docker compose logs --tail=50 -f $(SVC)

# ──────────────────────────────────────────────
# II. Kafka
# ──────────────────────────────────────────────
.PHONY: kafka-topics kafka-publish kafka-publish-full

kafka-topics:                   ## Create topics (taxi.trip.events, .invalid, .dlq)
	docker compose run --rm topic-init

kafka-publish:                  ## Publish events to Kafka (default 5000 events)
	docker compose run --rm \
	  -e TOPIC="$${TOPIC:-taxi.trip.events.$$(date +%s)}" \
	  -e MAX_EVENTS="$${MAX_EVENTS:-5000}" \
	  -e INVALID_RATE="$${INVALID_RATE:-0.02}" \
	  generator

kafka-publish-full:             ## Publish ALL 9.5M events (takes hours)
	MAX_EVENTS=-1 INVALID_RATE=0.01 $(MAKE) kafka-publish

# ──────────────────────────────────────────────

# ──────────────────────────────────────────────
# CDC. Debezium (Postgres → Kafka → events)
# ──────────────────────────────────────────────
.PHONY: cdc-up cdc-seed cdc-register cdc-bridge cdc-verify

cdc-up:                         ## Start Postgres + Debezium
	docker compose --profile tools up -d nyc_postgres debezium

cdc-seed:                        ## Seed Postgres trips table from parquet (5000 rows)
	docker compose --profile tools run --rm cdc-seed

cdc-seed-full:                   ## Seed Postgres with 50K rows
	docker compose --profile tools run --rm cdc-seed --max-rows 50000

cdc-register:                    ## Register Debezium Postgres connector
	docker compose --profile tools run --rm cdc-register

cdc-bridge:                      ## Run CDC bridge: Debezium topic → taxi.trip.events
	docker compose --profile tools run --rm cdc-bridge

cdc-verify:                      ## CDC E2E: seed → register → bridge → verify
	$(MAKE) cdc-seed
	@echo "=== 2/4 Register connector ==="
	$(MAKE) cdc-register
	@echo "=== 3/4 Bridge (500 events) ==="
	docker compose --profile tools run --rm cdc-bridge --max-events 500
	@echo "=== 4/4 Verify via Spark batch (optional, run make verify-all after) ==="

# ──────────────────────────────────────────────
# III. Spark
spark-batch:                    ## Batch backfill from parquet (fast, no Kafka needed)
	docker run --rm \
	  -v "$(PWD):/opt/project" \
	  -w /opt/project \
	  --entrypoint /opt/spark/bin/spark-submit \
	  apache/spark:3.5.1 \
	  --master local[*] \
	  /opt/project/jobs/spark_local_batch.py \
	  --input "/opt/project/data/raw/yellow_taxi/year=2024/month=$${MONTH:-01}/yellow_tripdata_2024-$${MONTH:-01}.parquet" \
	  --lookup "/opt/project/data/lookup/taxi_zone_lookup.csv"

spark-streaming:                ## Submit streaming job to Spark master (from Kafka)
	TOPIC="$${TOPIC:-taxi.trip.events}" bash scripts/start_streaming_job_docker.sh

# ──────────────────────────────────────────────
# IV. Trino
# ──────────────────────────────────────────────
.PHONY: trino-bootstrap trino-shell

trino-bootstrap:                ## Register tables (trips, invalid_trips) from silver parquet
	docker compose --profile tools --profile trino run --rm trino-bootstrap

trino-shell:                    ## Interactive Trino shell
	@docker exec -it nyc_trino trino --user analytics

# ──────────────────────────────────────────────
# V. dbt
# ──────────────────────────────────────────────
.PHONY: dbt-build dbt-run dbt-test dbt-debug

dbt-build:                      ## Full dbt build: models + tests
	docker compose --profile tools --profile dbt run --rm dbt dbt build

dbt-run:                        ## Run models only (skip tests)
	docker compose --profile tools --profile dbt run --rm dbt dbt run

dbt-test:                       ## Run tests only
	docker compose --profile tools --profile dbt run --rm dbt dbt test

dbt-debug:                      ## dbt build with debug output
	docker compose --profile tools --profile dbt run --rm dbt dbt build --debug

# ──────────────────────────────────────────────
# VI. Superset
# ──────────────────────────────────────────────
.PHONY: superset-bootstrap superset-check superset-open

superset-bootstrap:             ## Register DB, dataset, 4 charts, dashboard (idempotent)
	docker exec -i nyc_superset python3 < scripts/superset_bootstrap.py

superset-check:                 ## List Superset resources
	docker exec -i nyc_superset python3 < scripts/superset_check.py

superset-open:                  ## Open Superset UI
	@echo "Open http://localhost:8088  (admin/admin) -> dashboard 'NYC Taxi Overview'"

# ──────────────────────────────────────────────
# VII. Airflow
# ──────────────────────────────────────────────
.PHONY: airflow-up airflow-dags airflow-trigger airflow-test-task airflow-open

airflow-up:                     ## Start Airflow (requires infra-up first)
	docker compose --profile airflow up -d

airflow-dags:                   ## List DAGs
	@docker exec nyc_airflow_webserver airflow dags list 2>/dev/null || echo "Airflow not ready, try: make infra-up-all"

airflow-trigger:                ## Trigger a DAG (usage: make airflow-trigger DAG=nyc_analytics_refresh)
	@docker exec nyc_airflow_webserver airflow dags trigger $(DAG)

airflow-test-task:              ## Test a single task (usage: make airflow-test-task DAG=nyc_e2e_pipeline TASK=dbt_build)
	@docker exec nyc_airflow_webserver airflow tasks test $(DAG) $(TASK) $$(date +%Y-%m-%d)

airflow-open:                   ## Open Airflow UI
	@echo "Open http://localhost:8080  (admin/admin)"

# ──────────────────────────────────────────────
# VIII. Verify
# ──────────────────────────────────────────────
.PHONY: verify-mart verify-analytics verify-e2e verify-all

verify-mart:                    ## Row counts of all mart tables in Trino
	python3 scripts/verify_mart.py

verify-analytics:               ## Run 10 analytics SQL questions (expect PASS 10/10)
	python3 scripts/run_analytics_questions.py

verify-e2e:                     ## Full Kafka E2E test (~1000 events)
	MAX_EVENTS=5000 bash scripts/local_e2e_test.sh

verify-e2e-full:                ## Full 9.5M E2E test (resource heavy, long running)
	bash scripts/local_e2e_full_9_5m.sh

verify-all:                     ## Full pipeline: batch -> Trino -> dbt -> analytics -> Superset
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

# ──────────────────────────────────────────────
# IX. Clean
# ──────────────────────────────────────────────
.PHONY: setup-volumes clean-silver clean-quarantine clean-checkpoints clean-metastore clean-all

clean-silver:                   ## Delete silver parquet data
	docker run --rm -v "$(PWD):/opt/project" --user root alpine:latest sh -c "rm -rf /opt/project/data/silver/trips/* 2>/dev/null; echo cleaned"

clean-quarantine:               ## Delete quarantine parquet data
	rm -rf data/quarantine/invalid_trips/*

clean-checkpoints:              ## Delete streaming checkpoints
	rm -rf data/checkpoints/*

clean-metastore:                ## Reset Trino HMS metastore
	rm -rf data/trino-metastore/*

## One-time: Fix data dir permissions for Docker (Spark runs as uid 185, host as uid 1000)
setup-volumes:
	docker run --rm -v "$(PWD):/opt/project" --user root alpine:latest sh -c "rm -rf /opt/project/data/silver/trips /opt/project/data/silver/trips_local_test; mkdir -p /opt/project/data/silver/trips /opt/project/data/quarantine/invalid_trips; chmod 777 /opt/project/data/silver/trips /opt/project/data/quarantine/invalid_trips /opt/project/data/trino-metastore /opt/project/data/checkpoints 2>/dev/null; echo 'Done: data dirs 777'"
	-chmod 777 data/trino-metastore data/checkpoints 2>/dev/null

## Clean everything (use docker root to delete leftover files)
clean-all: clean-checkpoints clean-metastore
	@docker run --rm -v "$(PWD):/opt/project" --user root alpine:latest sh -c "rm -rf /opt/project/data/silver/trips /opt/project/data/quarantine/invalid_trips 2>/dev/null; echo 'cleaned'"
	mkdir -p data/silver/trips data/quarantine/invalid_trips
	chmod 777 data/silver/trips data/quarantine/invalid_trips
	@echo "All generated data cleaned"
