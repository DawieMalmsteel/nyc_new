#!/usr/bin/env python3
"""scripts/cdc_seed.py — Seed Postgres trips table from raw parquet.

Reads NYC yellow taxi parquet files and inserts rows into the nyc_postgres
trips table (used as source for Debezium CDC).

Usage:
    python3 scripts/cdc_seed.py \
        --input /opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet \
        --max-rows 5000

Connects to postgresql://postgres:postgres@nyc_postgres:5432/nyc_taxi
"""
import argparse
import sys

import pandas as pd
from sqlalchemy import create_engine, text

COLUMN_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input parquet file")
    parser.add_argument("--max-rows", type=int, default=5000, help="Max rows to seed")
    parser.add_argument("--dsn", default="postgresql://postgres:postgres@nyc_postgres:5432/nyc_taxi")
    args = parser.parse_args()

    print(f"[seed] reading {args.input}")
    df = pd.read_parquet(args.input)
    print(f"[seed] loaded {len(df)} rows from parquet")

    # Rename columns
    available = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=available)
    keep_cols = [v for v in available.values() if v in df.columns]
    df = df[keep_cols]

    # Drop rows where all key columns are null
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime", "vendor_id"], how="all")

    # Limit rows
    if args.max_rows and len(df) > args.max_rows:
        df = df.sample(n=args.max_rows, random_state=42)

    # Convert timestamp columns
    for col in ["pickup_datetime", "dropoff_datetime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    print(f"[seed] writing {len(df)} rows to Postgres ...")
    engine = create_engine(args.dsn)

    # Clear existing data
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE trips RESTART IDENTITY CASCADE"))

    df.to_sql("trips", engine, if_exists="append", index=False, method="multi")
    print(f"[seed] done: {len(df)} rows inserted into nyc_taxi.public.trips")
    return 0

if __name__ == "__main__":
    sys.exit(main())
