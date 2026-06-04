#!/usr/bin/env python3
import argparse
import json
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer


def load_zones(path: str) -> set[int]:
    df = pd.read_csv(path)
    return set(df["LocationID"].astype(int).tolist())


def to_ts(v):
    if v is None:
        return None
    ts = pd.to_datetime(v, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts


def validate(event: dict, zone_ids: set[int]) -> list[str]:
    errors: list[str] = []

    pickup_ts = to_ts(event.get("pickup_datetime"))
    dropoff_ts = to_ts(event.get("dropoff_datetime"))

    if pickup_ts is None:
        errors.append("pickup_datetime_null_or_invalid")
    if dropoff_ts is None:
        errors.append("dropoff_datetime_null_or_invalid")
    if pickup_ts is not None and dropoff_ts is not None and not (dropoff_ts > pickup_ts):
        errors.append("invalid_trip_duration")

    trip_distance = event.get("trip_distance")
    if trip_distance is None or float(trip_distance) <= 0:
        errors.append("trip_distance_must_be_gt_0")

    fare_amount = event.get("fare_amount")
    total_amount = event.get("total_amount")
    if fare_amount is None or float(fare_amount) < 0:
        errors.append("fare_amount_must_be_gte_0")
    if total_amount is None:
        errors.append("total_amount_null")
    elif fare_amount is not None and float(total_amount) < float(fare_amount):
        errors.append("total_amount_must_be_gte_fare_amount")

    passenger_count = event.get("passenger_count")
    if passenger_count is None or not (1 <= int(passenger_count) <= 6):
        errors.append("passenger_count_out_of_range")

    pu = event.get("pickup_location_id")
    do = event.get("dropoff_location_id")
    if pu is None or int(pu) not in zone_ids:
        errors.append("pickup_location_not_found")
    if do is None or int(do) not in zone_ids:
        errors.append("dropoff_location_not_found")

    if not event.get("event_id"):
        errors.append("event_id_null")

    return errors


def write_partitioned_parquet(df: pd.DataFrame, base_path: str, partition_cols: list[str]) -> None:
    if df.empty:
        return
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(table, root_path=base_path, partition_cols=partition_cols)


def write_single_parquet(df: pd.DataFrame, base_path: str) -> None:
    if df.empty:
        return
    os.makedirs(base_path, exist_ok=True)
    file_name = f"part-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, os.path.join(base_path, file_name))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    parser.add_argument("--topic", default="taxi.trip.events")
    parser.add_argument("--lookup-path", default="data/lookup/taxi_zone_lookup.csv")
    parser.add_argument("--silver-path", default="data/silver/trips")
    parser.add_argument("--quarantine-path", default="data/quarantine/invalid_trips")
    parser.add_argument("--group-id", default="nyc-local-processor")
    parser.add_argument("--max-empty-polls", type=int, default=8)
    parser.add_argument("--poll-timeout-ms", type=int, default=1500)
    args = parser.parse_args()

    zone_ids = load_zones(args.lookup_path)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
        group_id=args.group_id,
    )

    rows = []
    empty_polls = 0

    while True:
        polled = consumer.poll(timeout_ms=args.poll_timeout_ms)
        if not polled:
            empty_polls += 1
            if empty_polls >= args.max_empty_polls:
                break
            continue

        empty_polls = 0

        for tp, messages in polled.items():
            for msg in messages:
                event = msg.value
                errors = validate(event, zone_ids)

                pickup_ts = to_ts(event.get("pickup_datetime"))
                pickup_year = int(pickup_ts.year) if pickup_ts is not None else None
                pickup_month = int(pickup_ts.month) if pickup_ts is not None else None
                pickup_date = pickup_ts.date().isoformat() if pickup_ts is not None else None
                pickup_hour = int(pickup_ts.hour) if pickup_ts is not None else None

                event["kafka_topic"] = msg.topic
                event["kafka_partition"] = msg.partition
                event["kafka_offset"] = msg.offset
                event["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
                event["pickup_year"] = pickup_year
                event["pickup_month"] = pickup_month
                event["pickup_date"] = pickup_date
                event["pickup_hour"] = pickup_hour
                event["validation_errors"] = errors
                event["quarantine_ts"] = datetime.now(timezone.utc).isoformat() if errors else None
                event["is_valid"] = len(errors) == 0

                rows.append(event)

    consumer.close()

    if not rows:
        print("[warn] No messages consumed")
        return

    df = pd.DataFrame(rows)

    valid_df = df[df["is_valid"] == True].copy()
    invalid_df = df[df["is_valid"] == False].copy()

    # Keep only rows with partition fields for silver sink
    valid_df = valid_df[valid_df["pickup_year"].notna() & valid_df["pickup_month"].notna()].copy()
    if not valid_df.empty:
        valid_df["pickup_year"] = valid_df["pickup_year"].astype(int)
        valid_df["pickup_month"] = valid_df["pickup_month"].astype(int)

    write_partitioned_parquet(valid_df, args.silver_path, ["pickup_year", "pickup_month"])
    write_single_parquet(invalid_df, args.quarantine_path)

    print(f"[done] consumed={len(df)} valid={len(valid_df)} invalid={len(invalid_df)}")


if __name__ == "__main__":
    main()
