# nyc-pipeline-tools
# One-shot CLI container for: topic-init, quality-report, CDC bridge/seed/register.
# Mounted project lives at /opt/project (same convention as spark services).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/project



# CDC bridge/seed dependencies
RUN pip install --no-cache-dir psycopg2-binary sqlalchemy kafka-python trino pandas pyarrow
# Copy entrypoint scripts and create symlinks (remove .sh suffix for K8s compat)
COPY docker/entrypoint-*.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint-*.sh && \
    for f in /usr/local/bin/entrypoint-*.sh; do ln -s "$f" "${f%.sh}"; done


CMD ["bash"]
