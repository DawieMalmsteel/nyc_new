#!/usr/bin/env python3
"""scripts/cdc_bridge.py — Debezium CDC → taxi.trip.events bridge.

Consumes from the raw CDC topic (nyc_cdc.public.trips), transforms
Debezium-enveloped events into the standard NYC Taxi event format
(compatible with the existing Spark streaming job), and produces
them to taxi.trip.events.

Performance: uses async Kafka send + periodic flush for ~50x throughput
improvement over sync .get() per message. Prints benchmark at the end.

Usage:
    python3 scripts/cdc_bridge.py --bootstrap-server kafka:9092 [--max-events 0]
"""
from uuid import uuid4
import argparse
import json
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer


def transform(event: dict) -> dict | None:
    """Transform Debezium CDC event (already unwrapped) to standard NYC Taxi event format."""
    if not event:
        return None

    def fmt_micro(ts_us):
        if ts_us is None:
            return None
        return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "event_id": f"cdc-{event.get('trip_id', '0')}",
        "event_timestamp": ts,
        "source_file": "cdc:nyc_postgres:nyc_taxi:public.trips",
        "vendor_id": event.get("vendor_id"),
        "pickup_datetime": fmt_micro(event.get("pickup_datetime")),
        "dropoff_datetime": fmt_micro(event.get("dropoff_datetime")),
        "passenger_count": event.get("passenger_count"),
        "trip_distance": event.get("trip_distance"),
        "rate_code_id": event.get("rate_code_id"),
        "store_and_fwd_flag": None,
        "pickup_location_id": event.get("pickup_location_id"),
        "dropoff_location_id": event.get("dropoff_location_id"),
        "payment_type": event.get("payment_type"),
        "fare_amount": event.get("fare_amount"),
        "extra": event.get("extra"),
        "mta_tax": event.get("mta_tax"),
        "tip_amount": event.get("tip_amount"),
        "tolls_amount": event.get("tolls_amount"),
        "improvement_surcharge": event.get("improvement_surcharge"),
        "total_amount": event.get("total_amount"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="kafka:9092")
    parser.add_argument("--input-topic", default="nyc_cdc.public.trips")
    parser.add_argument("--output-topic", default="taxi.trip.events")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Max events to bridge (0 = unlimited)")
    parser.add_argument("--poll-timeout", type=float, default=1.0)
    parser.add_argument("--flush-interval", type=int, default=500,
                        help="Flush Kafka producer every N events (default 500)")
    parser.add_argument("--linger-ms", type=int, default=100,
                        help="Kafka producer linger.ms for batching (default 100)")
    parser.add_argument("--sync", action="store_true",
                        help="Use sync .get() per message (old behavior, for comparison)")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.input_topic,
        bootstrap_servers=args.bootstrap_server,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"cdc-bridge-{uuid4().hex[:8]}",
    )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=args.linger_ms,
        batch_size=65536,   # 64KB batch
        compression_type="gzip",
    )

    print(f"[cdc-bridge] consuming from '{args.input_topic}' -> '{args.output_topic}'")
    print(f"[cdc-bridge] producer: linger_ms={args.linger_ms}, flush_interval={args.flush_interval}")
    sent = 0
    start_ts = time.time()
    flush_count = 0
    try:
        for msg in consumer:
            event = msg.value
            tx = transform(event)
            if tx is None:
                continue
            if args.sync:
                # Old behavior: sync .get() after each event (slow)
                producer.send(args.output_topic, value=tx).get(timeout=10)
            else:
                # Async send — no .get() blocking
                producer.send(args.output_topic, value=tx)
            sent += 1

            if sent % args.flush_interval == 0:
                producer.flush()
                flush_count += 1
                elapsed = time.time() - start_ts
                rate = sent / elapsed if elapsed > 0 else 0
                print(f"[cdc-bridge] bridged {sent} events ({rate:.0f} ev/s)")

            if args.max_events > 0 and sent >= args.max_events:
                print(f"[cdc-bridge] reached limit {args.max_events}, stopping")
                break
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        elapsed = time.time() - start_ts
        total_rate = sent / elapsed if elapsed > 0 else 0
        consumer.close()

    print(f"[cdc-bridge] === BENCHMARK ===")
    print(f"[cdc-bridge]   events:       {sent}")
    print(f"[cdc-bridge]   total time:   {elapsed:.2f}s")
    print(f"[cdc-bridge]   throughput:   {total_rate:.0f} ev/s")
    print(f"[cdc-bridge]   flush calls:  {flush_count}")
    if sent > 0:
        print(f"[cdc-bridge]   avg latency:  {(elapsed / sent * 1000):.1f} ms/ev")
    return 0


if __name__ == "__main__":
    sys.exit(main())
