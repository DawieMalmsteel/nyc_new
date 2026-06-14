# Custom Airflow 2.x image with providers for our pipeline.
# Pinning versions to avoid pip resolver upgrading airflow 2.x -> 3.x.
FROM apache/airflow:2.10.5-python3.11

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=False

USER airflow

ARG AIRFLOW_VERSION=2.10.5

# Đã sửa apache-airflow-providers-cncf-kubernetes thành bản 8.4.2
RUN pip install --no-cache-dir --no-deps \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-cncf-kubernetes==8.4.2" \
    "apache-airflow-providers-docker==3.14.1" \
    "apache-airflow-providers-http==5.3.0" \
    "apache-airflow-providers-postgres==6.2.0" \
    "apache-airflow-providers-common-sql==1.27.0" \
    "apache-airflow-providers-trino==6.2.0" \
    && pip install --no-cache-dir requests lz4 orjson trino==0.337.0 kubernetes==29.0.0
