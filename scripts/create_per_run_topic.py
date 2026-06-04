#!/usr/bin/env python3
"""Create a per-run Kafka topic and wait until its partitions have leaders.

Usage:
  python3 scripts/create_per_run_topic.py <bootstrap-server> <topic> [num_partitions]
"""
import sys
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: create_per_run_topic.py <bootstrap> <topic> [partitions]", file=sys.stderr)
        return 2

    bootstrap = sys.argv[1]
    topic = sys.argv[2]
    partitions = int(sys.argv[3]) if len(sys.argv) > 3 else 3

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="nyc-e2e-topic")
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=partitions, replication_factor=1)]
        )
        print(f"[created] {topic}")
    except TopicAlreadyExistsError:
        print(f"[exists] {topic}")

    # Wait for partition leaders to be assigned cluster-wide.
    for _ in range(60):
        try:
            meta = admin.describe_topics([topic])
        except Exception:
            meta = None
        if meta and meta[0].get("partitions"):
            parts = meta[0]["partitions"]
            if parts and all(p.get("leader") is not None for p in parts):
                print(f"[ready] {topic} ({len(parts)} partitions, leaders assigned)")
                admin.close()
                return 0
        time.sleep(1)

    print(f"[timeout] {topic} not ready after 60s", file=sys.stderr)
    admin.close()
    return 1


if __name__ == "__main__":
    sys.exit(main())
