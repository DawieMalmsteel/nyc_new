# Custom Airflow 2.x image with providers for our pipeline.
# Pinning versions to avoid pip resolver upgrading airflow 2.x -> 3.x.
FROM apache/airflow:2.10.5-python3.11
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__LOAD_EXAMPLES=False
RUN apt-get update || true; \
    apt-get install -y --no-install-recommends curl ca-certificates || true; \
    rm -rf /var/lib/apt/lists/* || true; \
    curl -fsSL https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64 \
        -o /usr/local/bin/docker-compose || true; \
    chmod +x /usr/local/bin/docker-compose || true; \
    ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose || true
# from pulling newer transitive deps that would force an airflow 3.x upgrade.
ARG AIRFLOW_VERSION=2.10.5
RUN pip install --no-cache-dir --no-deps \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-docker==3.14.1" \
    "apache-airflow-providers-http==5.3.0" \
    "apache-airflow-providers-postgres==6.2.0" \
    "apache-airflow-providers-common-sql==1.27.0" \
    "apache-airflow-providers-trino==6.2.0" \
    && pip install --no-cache-dir requests lz4 orjson trino==0.337.0
