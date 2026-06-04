#!/usr/bin/env bash
set -euo pipefail
python3 scripts/create_kafka_topics.py --bootstrap-server "${1:-localhost:29092}"
