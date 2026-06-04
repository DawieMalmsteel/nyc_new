#!/usr/bin/env python3
import argparse
import glob
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaProducer


COLUMN_ALIASES = {
    "vendorid": "vendor_id",
    "vendor_id": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
    "ratecodeid": "rate_code_id",
}


def normalize_columns(columns: list[str]) -> list[str]:
    normalized: list[str] = []
    for col in columns:
        c = col.strip().lower()
        normalized.append(COLUMN_ALIASES.get(c, c))
    return normalized


def to_iso(value) -> str | None:
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    # Spark-friendly format
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def inject_invalid(event: dict) -> dict:
    mode = random.choice(["neg_distance", "null_pickup", "bad_passenger", "bad_total"])
    if mode == "neg_distance":
        event["trip_distance"] = -abs(float(event.get("trip_distance") or 1.0))
    elif mode == "null_pickup":
        event["pickup_datetime"] = None
    elif mode == "bad_passenger":
        event["passenger_count"] = 0
    elif mode == "bad_total":
        fare = float(event.get("fare_amount") or 0.0)
        event["total_amount"] = fare - 1.0
    return event


def py_val(v):
    if pd.isna(v):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return v


def build_event(record: dict, source_file: str) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source_file": source_file,
        "vendor_id": int(py_val(record.get("vendor_id"))) if py_val(record.get("vendor_id")) is not None else None,
        "pickup_datetime": to_iso(py_val(record.get("pickup_datetime"))),
        "dropoff_datetime": to_iso(py_val(record.get("dropoff_datetime"))),
        "passenger_count": int(py_val(record.get("passenger_count"))) if py_val(record.get("passenger_count")) is not None else None,
        "trip_distance": float(py_val(record.get("trip_distance"))) if py_val(record.get("trip_distance")) is not None else None,
        "rate_code_id": int(py_val(record.get("rate_code_id"))) if py_val(record.get("rate_code_id")) is not None else None,
        "store_and_fwd_flag": py_val(record.get("store_and_fwd_flag")),
        "pickup_location_id": int(py_val(record.get("pickup_location_id"))) if py_val(record.get("pickup_location_id")) is not None else None,
        "dropoff_location_id": int(py_val(record.get("dropoff_location_id"))) if py_val(record.get("dropoff_location_id")) is not None else None,
        "payment_type": int(py_val(record.get("payment_type"))) if py_val(record.get("payment_type")) is not None else None,
        "fare_amount": float(py_val(record.get("fare_amount"))) if py_val(record.get("fare_amount")) is not None else None,
        "extra": float(py_val(record.get("extra"))) if py_val(record.get("extra")) is not None else 0.0,
        "mta_tax": float(py_val(record.get("mta_tax"))) if py_val(record.get("mta_tax")) is not None else 0.0,
        "tip_amount": float(py_val(record.get("tip_amount"))) if py_val(record.get("tip_amount")) is not None else 0.0,
        "tolls_amount": float(py_val(record.get("tolls_amount"))) if py_val(record.get("tolls_amount")) is not None else 0.0,
        "improvement_surcharge": float(py_val(record.get("improvement_surcharge"))) if py_val(record.get("improvement_surcharge")) is not None else 0.0,
        "total_amount": float(py_val(record.get("total_amount"))) if py_val(record.get("total_amount")) is not None else None,
    }
    return event


def resolve_parquet_files(inputs: list[str]) -> list[str]:
    files: list[str] = []
    for item in inputs:
        p = Path(item)
        if "*" in item or "?" in item or "[" in item:
            files.extend(glob.glob(item, recursive=True))
        elif p.is_dir():
            files.extend([str(x) for x in p.rglob("*.parquet")])
        elif p.is_file() and p.suffix == ".parquet":
            files.append(str(p))
    files = sorted(set(files))
    if not files:
        raise FileNotFoundError(f"No parquet files found from input: {inputs}")
    return files


def iter_records_from_file(parquet_file: str, batch_size: int):
    pf = pq.ParquetFile(parquet_file)
    for rb in pf.iter_batches(batch_size=batch_size):
        df = rb.to_pandas()
        df.columns = normalize_columns(list(df.columns))
        for record in df.to_dict(orient="records"):
            yield record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", nargs="+", required=True, help="Parquet file(s), folder(s), or glob pattern(s)")
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    parser.add_argument("--topic", default="taxi.trip.events")
    parser.add_argument("--events-per-second", type=float, default=0.0)
    parser.add_argument("--max-events", type=int, default=-1, help="-1 means send all records")
    parser.add_argument("--invalid-rate", type=float, default=0.01)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--flush-every", type=int, default=5000)
    args = parser.parse_args()

    parquet_files = resolve_parquet_files(args.input)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        acks="all",
    )

    sent = 0
    delay = 0.0 if args.events_per_second <= 0 else 1.0 / args.events_per_second

    print(f"[info] files={len(parquet_files)} max_events={args.max_events} eps={args.events_per_second}")
    for parquet_file in parquet_files:
        source_file = os.path.basename(parquet_file)
        for record in iter_records_from_file(parquet_file, args.batch_size):
            if args.max_events > 0 and sent >= args.max_events:
                producer.flush()
                producer.close()
                print(f"[done] published {sent} events to {args.topic}")
                return

            event = build_event(record, source_file)
            if random.random() < args.invalid_rate:
                event = inject_invalid(event)

            producer.send(args.topic, value=event)
            sent += 1

            if sent % args.flush_every == 0:
                producer.flush()
                print(f"[progress] sent={sent}")

            if delay > 0:
                time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"[done] published {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
