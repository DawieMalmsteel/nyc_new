# nyc-dbt: dbt-trino image.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip install "dbt-trino>=1.7,<2.0"

COPY docker/entrypoint-dbt.sh /usr/local/bin/entrypoint-dbt
RUN chmod +x /usr/local/bin/entrypoint-dbt

WORKDIR /opt/project/dbt

CMD ["bash"]
