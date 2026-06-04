# nyc-pipeline-tools
# One-shot CLI container for: topic-init, generator, quality-report.
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

COPY generator/requirements.txt /opt/project/generator/requirements.txt
RUN pip install -r /opt/project/generator/requirements.txt

# k8s-style wait-for helper.
COPY docker/wait-kafka.sh /usr/local/bin/wait-kafka
RUN chmod +x /usr/local/bin/wait-kafka

COPY docker/entrypoint-topic-init.sh     /usr/local/bin/entrypoint-topic-init
COPY docker/entrypoint-generator.sh     /usr/local/bin/entrypoint-generator
COPY docker/entrypoint-quality.sh        /usr/local/bin/entrypoint-quality
COPY docker/entrypoint-trino-bootstrap.sh /usr/local/bin/entrypoint-trino-bootstrap
RUN chmod +x /usr/local/bin/entrypoint-*

CMD ["bash"]
