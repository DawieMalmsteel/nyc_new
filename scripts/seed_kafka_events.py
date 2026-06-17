#!/usr/bin/env python3
"""
Seed Kafka topic 'taxi.trip.events' directly from raw parquet.
No Postgres/Debezium needed — reads parquet, publishes JSON events.
Usage: python3 seed_kafka_events.py --input <parquet_path> --bootstrap-server <kafka> --max-rows 1000
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed Kafka with taxi trip events from parquet")
    p.add_argument("--input", default="/opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet")
    p.add_argument("--bootstrap-server", default="svc-kafka:9092")
    p.add_argument("--topic", default="taxi.trip.events")
    p.add_argument("--max-rows", type=int, default=1000)
    p.add_argument("--batch-size", type=int, default=100)
    return p.parse_args()


def produce_batch(producer: KafkaProducer, topic: str, events: list[dict]) -> None:
    for ev in events:
        producer.send(topic, value=ev)
    producer.flush()
    print(f"  published {len(events)} events")


def main() -> None:
    args = parse_args()

    if not os.path.exists(args.input):
        print(f"[seed-kafka] parquet not found: {args.input}, trying s3a path via pandas...")
        df = pd.read_parquet(args.input)
    else:
        df = pd.read_parquet(args.input)

    df = df.head(args.max_rows)
    total = len(df)
    print(f"[seed-kafka] loaded {total} rows from {args.input}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=10_000_000,
    )

    date_cols = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "pickup_datetime", "dropoff_datetime",
        "lpep_pickup_datetime", "lpep_dropoff_datetime",
    ]
    published = 0
    events: list[dict] = []

    for _, row in df.iterrows():
        pickup = None
        dropoff = None
        for c in date_cols:
            if c in df.columns and pd.notna(row.get(c)):
                val = row[c]
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                if "pickup" in c.lower() and pickup is None:
                    pickup = str(val)
                elif "dropoff" in c.lower() and dropoff is None:
                    dropoff = str(val)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_file": os.path.basename(args.input),
            "vendor_id": int(row.get("VendorID", row.get("vendor_id", 1))) if pd.notna(row.get("VendorID", row.get("vendor_id"))) else 1,
            "pickup_datetime": pickup or "",
            "dropoff_datetime": dropoff or "",
            "passenger_count": int(row.get("passenger_count", 1)) if pd.notna(row.get("passenger_count")) else 1,
            "trip_distance": float(row.get("trip_distance", 0.0)) if pd.notna(row.get("trip_distance")) else 0.0,
            "rate_code_id": int(row.get("RatecodeID", row.get("rate_code_id", 1))) if pd.notna(row.get("RatecodeID", row.get("rate_code_id"))) else 1,
            "store_and_fwd_flag": str(row.get("store_and_fwd_flag", "N")) if pd.notna(row.get("store_and_fwd_flag")) else "N",
            "pickup_location_id": int(row.get("PULocationID", row.get("pickup_location_id", 1))) if pd.notna(row.get("PULocationID", row.get("pickup_location_id"))) else 1,
            "dropoff_location_id": int(row.get("DOLocationID", row.get("dropoff_location_id", 1))) if pd.notna(row.get("DOLocationID", row.get("dropoff_location_id"))) else 1,
            "payment_type": int(row.get("payment_type", 1)) if pd.notna(row.get("payment_type")) else 1,
            "fare_amount": float(row.get("fare_amount", 0.0)) if pd.notna(row.get("fare_amount")) else 0.0,
            "extra": float(row.get("extra", 0.0)) if pd.notna(row.get("extra")) else 0.0,
            "mta_tax": float(row.get("mta_tax", 0.0)) if pd.notna(row.get("mta_tax")) else 0.0,
            "tip_amount": float(row.get("tip_amount", 0.0)) if pd.notna(row.get("tip_amount")) else 0.0,
            "tolls_amount": float(row.get("tolls_amount", 0.0)) if pd.notna(row.get("tolls_amount")) else 0.0,
            "improvement_surcharge": float(row.get("improvement_surcharge", 0.0)) if pd.notna(row.get("improvement_surcharge")) else 0.0,
            "total_amount": float(row.get("total_amount", 0.0)) if pd.notna(row.get("total_amount")) else 0.0,
        }
        events.append(event)
        published += 1

        if len(events) >= args.batch_size:
            produce_batch(producer, args.topic, events)
            events = []

    if events:
        produce_batch(producer, args.topic, events)

    print(f"[seed-kafka] DONE: {published} events published to {args.topic}")
    producer.close()


if __name__ == "__main__":
    main()
