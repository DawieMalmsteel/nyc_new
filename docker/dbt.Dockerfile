# nyc-dbt: dbt-trino image.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120

# Install dbt-trino with retry on network issues
RUN pip install --retries 5 "dbt-trino>=1.7,<2.0"

# Copy entrypoint script for K8s jobs
COPY docker/entrypoint-dbt.sh /usr/local/bin/entrypoint-dbt
RUN chmod +x /usr/local/bin/entrypoint-dbt

WORKDIR /opt/project/dbt

CMD ["bash"]
