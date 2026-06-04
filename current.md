# Current State - NYC Taxi Pipeline (2026-06-04)

## 1. Accomplishments (Pipeline Dockerized)
- **Kafka + Spark + MinIO**: Fully dockerized, e2e test (1000 events) PASS.
- **Trino + dbt + Superset**:
  - Trino setup: `hive` catalog, `trips` & `invalid_trips` tables.
  - dbt project: 6 models (stg/dim/fact/mart) + 9 tests (build PASS 15/15).
  - Analytics layer: 10 SQL questions PASS (10/10).
  - Superset: 1 DB, 1 dataset, 4 charts, 1 dashboard.

## 2. In-Progress (Airflow Orchestration)
- **Airflow Setup**: Airflow 2.10.5 running, Postgres metadata, custom image.
- **Issues**:
  - Scheduler stuck on `SequentialExecutor` instead of `LocalExecutor` (DAGs queued).
  - Permission issues with `docker.sock` in Airflow container (fixed by adding group 958, but execution environment inside Airflow still struggling with path/permission issues for `docker run`).

## 3. Blockers
- **Scheduler**: Airflow DAGs remain in `queued` state due to `SequentialExecutor`.
- **Task Execution**: `BashOperator` / `DockerOperator` in Airflow fails with `rc=126` (Permission denied) or `AirflowException` (Failed to establish connection to Docker host), despite `docker` CLI working from shell.

## 4. Next Steps
1. Force Airflow to use `LocalExecutor` by resetting metadata/config.
2. Debug Docker connectivity from Airflow operators (ensure permissions/socket paths are consistent).
3. Validate full end-to-end DAG execution.
