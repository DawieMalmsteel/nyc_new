# nyc-pipeline-tools
# One-shot CLI container for: topic-init, quality-report, CDC bridge/seed/register.
# Mounted project lives at /opt/project (same convention as spark services).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/project

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*


# CDC bridge/seed dependencies
RUN pip install --no-cache-dir psycopg2-binary sqlalchemy kafka-python trino
# k8s-style wait-for helper.
COPY docker/wait-kafka.sh /usr/local/bin/wait-kafka
RUN chmod +x /usr/local/bin/wait-kafka

COPY docker/entrypoint-topic-init.sh     /usr/local/bin/entrypoint-topic-init
COPY docker/entrypoint-quality.sh        /usr/local/bin/entrypoint-quality
COPY docker/entrypoint-trino-bootstrap.sh /usr/local/bin/entrypoint-trino-bootstrap
COPY docker/entrypoint-cdc-bridge.sh      /usr/local/bin/entrypoint-cdc-bridge
COPY docker/entrypoint-cdc-seed.sh        /usr/local/bin/entrypoint-cdc-seed
COPY docker/entrypoint-cdc-register.sh    /usr/local/bin/entrypoint-cdc-register
RUN chmod +x /usr/local/bin/entrypoint-*

CMD ["bash"]
