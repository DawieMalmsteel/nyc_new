#!/usr/bin/env python3
import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args()

    topics = [
        "taxi.trip.events",
        "taxi.trip.invalid",
        "taxi.trip.dlq",
    ]

    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_server, client_id="nyc-topic-admin")
    for topic in topics:
        try:
            admin.create_topics(
                [
                    NewTopic(
                        name=topic,
                        num_partitions=args.partitions,
                        replication_factor=args.replication_factor,
                    )
                ],
                validate_only=False,
            )
            print(f"[created] {topic}")
        except TopicAlreadyExistsError:
            print(f"[exists] {topic}")
    admin.close()


if __name__ == "__main__":
    main()
