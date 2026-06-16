#!/usr/bin/env python3
"""
spark_local_batch.py

Spark local[*] batch processor that mirrors the streaming job's enrichment logic.
Reads raw parquet + taxi_zone_lookup, produces enriched silver/quarantine parquet
compatible with Trino + dbt pipeline.

Usage:
    docker run --rm -v $(pwd):/opt/project -w /opt/project \
      --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.1 \
      --master local[*] /opt/project/jobs/spark_local_batch.py \
      --input "/opt/project/data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet" \
      --lookup "/opt/project/data/lookup/taxi_zone_lookup.csv" \
      --silver "/opt/project/data/silver/trips" \
      --quarantine "/opt/project/data/quarantine/invalid_trips"
"""
import os
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

_NOT_ZONE = ["Unknown", "N/A", "NV"]


def run_batch(input_path, lookup_path, silver_path, quarantine_path,
              expected_year=None, expected_month=None):
    print(f"Starting enriched batch")
    print(f"  input:      {input_path}")
    print(f"  lookup:     {lookup_path}")
    print(f"  silver:     {silver_path}")
    print(f"  quarantine: {quarantine_path}")

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minio")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minio123")
    spark = SparkSession.builder \
        .appName("LocalBatchEnriched") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # --- 1. Read raw parquet ---
    raw = spark.read.parquet(input_path)

    zones_raw = spark.read.option("header", "true").csv(lookup_path)
    zones = zones_raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.when(F.col("Borough").isin(*_NOT_ZONE), F.lit(None))
         .otherwise(F.col("Borough")).alias("borough"),
        F.when(F.col("Zone").isin(*_NOT_ZONE), F.lit(None))
         .otherwise(F.col("Zone")).alias("zone"),
        F.when(F.col("service_zone").isin(*_NOT_ZONE), F.lit(None))
         .otherwise(F.col("service_zone")).alias("service_zone"),
    )
    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    # --- 3. Enrich ---
    enriched = raw.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.to_timestamp("tpep_pickup_datetime").alias("pickup_ts"),
        F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.col("passenger_count").cast("int"),
        F.col("trip_distance").cast("double"),
        F.col("RatecodeID").cast("int").alias("rate_code_id"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("payment_type").cast("int"),
        F.col("fare_amount").cast("double"),
        F.col("extra").cast("double"),
        F.col("mta_tax").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("tolls_amount").cast("double"),
        F.col("improvement_surcharge").cast("double"),
        F.col("total_amount").cast("double"),
    )

    # Add trip_id (hash of pickup_ts + pickup_loc + dropoff_loc) + metadata columns
    enriched = enriched \
        .withColumn("trip_id",
            F.xxhash64(F.concat_ws("|",
                F.col("pickup_ts").cast("string"),
                F.col("pickup_location_id").cast("string"),
                F.col("dropoff_location_id").cast("string")
            ))) \
        .withColumn("event_ts", F.current_timestamp()) \
        .withColumn("ingestion_ts", F.current_timestamp()) \
        .withColumn("pickup_date", F.to_date(F.col("pickup_ts"))) \
        .withColumn("pickup_hour", F.hour(F.col("pickup_ts"))) \
        .withColumn("pickup_year", F.year(F.col("pickup_ts"))) \
        .withColumn("pickup_month", F.month(F.col("pickup_ts")))

    # Derive expected year/month from each row's source file path (e.g. .../year=2024/month=01/...).
    # Use input_file_name() so glob reads resolve per-file.  Fall back to the explicit --year/--month
    # args when passed (single-file mode); otherwise filter edge rows from adjacent months.
    enriched = enriched \
        .withColumn("_src_file", F.input_file_name()) \
        .withColumn("_expected_year",
            F.regexp_extract(F.col("_src_file"), r"year=(\d{4})", 1).cast("int")) \
        .withColumn("_expected_month",
            F.regexp_extract(F.col("_src_file"), r"month=(\d{1,2})", 1).cast("int")) \
        .withColumn("source_file",
            F.element_at(F.split(F.col("_src_file"), "/"), -1))

    # --- 3b. Filter to expected year/month ---
    # Raw TLC parquet files often contain edge rows from adjacent months
    # (e.g. 2024-01 file includes late December 2023 trips).  Filter strictly
    # so silver partitions contain only data matching the file's target period.
    total_before = enriched.count()
    year_filter = F.col("pickup_year") == (F.lit(expected_year) if expected_year is not None else F.col("_expected_year"))
    month_filter = F.col("pickup_month") == (F.lit(expected_month) if expected_month is not None else F.col("_expected_month"))
    enriched = enriched.filter(year_filter & month_filter)
    filtered = total_before - enriched.count()
    if filtered > 0:
        yt = expected_year if expected_year is not None else "file"
        mt = f"{expected_month:02d}" if expected_month is not None else "file"
        print(f"  year/month filter dropped {filtered} rows "
              f"(expected {yt}-{mt}, kept {enriched.count()})")

    # Drop temp columns used only for filtering
    enriched = enriched.drop("_src_file", "_expected_year", "_expected_month")

    # Join zones
    enriched = enriched.join(pickup_zones, on="pickup_location_id", how="left")
    enriched = enriched.join(dropoff_zones, on="dropoff_location_id", how="left")

    # --- 4. Validate ---
    error_array = F.array(
        F.when(F.col("pickup_ts").isNull(), F.lit("pickup_datetime_null_or_invalid")),
        F.when(F.col("dropoff_ts").isNull(), F.lit("dropoff_datetime_null_or_invalid")),
        F.when(F.col("dropoff_ts") <= F.col("pickup_ts"), F.lit("invalid_trip_duration")),
        F.when(F.col("trip_distance") <= 0, F.lit("non_positive_trip_distance")),
        F.when(F.col("fare_amount") < 0, F.lit("negative_fare_amount")),
        F.when(F.col("total_amount") < F.col("fare_amount"), F.lit("total_amount_less_than_fare")),
        F.when(
            F.col("passenger_count").isNull() | F.col("passenger_count").between(0, 6).isNull() |
            ~F.col("passenger_count").between(1, 6),
            F.lit("invalid_passenger_count")
        ),
        F.when(
            F.col("payment_type").isNull() | (F.col("payment_type") < 1) | (F.col("payment_type") > 6),
            F.lit("payment_type_out_of_range")
        ),
        F.when(
            F.col("pickup_location_id").isNull() |
            (F.col("pickup_borough").isNull() & F.col("pickup_location_id").isNotNull()),
            F.lit("unknown_pickup_location")
        ),
        F.when(
            F.col("dropoff_location_id").isNull() |
            (F.col("dropoff_borough").isNull() & F.col("dropoff_location_id").isNotNull()),
            F.lit("unknown_dropoff_location")
        ),
    )

    validated = enriched \
        .withColumn("validation_error_candidates", error_array) \
        .withColumn("validation_errors",
                     F.expr("filter(validation_error_candidates, x -> x is not null)")) \
        .withColumn("is_valid", F.size(F.col("validation_errors")) == F.lit(0)) \
        .withColumn("quarantine_ts", F.current_timestamp())

    # --- 5. Split valid / invalid ---
    valid = validated.filter(F.col("is_valid"))
    invalid = validated.filter(~F.col("is_valid"))

    # Select columns for silver
    silver_columns = [
        "trip_id", "source_file",
        "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
        "rate_code_id", "pickup_location_id", "dropoff_location_id", "payment_type",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount",
        "pickup_borough", "pickup_zone", "pickup_service_zone",
        "dropoff_borough", "dropoff_zone", "dropoff_service_zone",
        "pickup_year", "pickup_month",
        "pickup_date", "pickup_hour",
        "event_ts", "ingestion_ts",
    ]
    # Write valid trips (partitioned by year/month)
    valid_count = valid.count()
    if valid_count > 0:
        valid.select(silver_columns) \
            .write.partitionBy("pickup_year", "pickup_month") \
            .mode("append") \
            .parquet(silver_path)
        print(f"Valid trips written: {valid_count}")
    else:
        print("Valid trips: 0")

    # Write invalid trips (non-partitioned)
    invalid_count = invalid.count()
    if invalid_count > 0:
        invalid.select(silver_columns + ["validation_errors", "quarantine_ts"]) \
            .write.mode("append") \
            .parquet(quarantine_path)
        print(f"Invalid trips written: {invalid_count}")
    else:
        print("Invalid trips: 0")

    spark.stop()
    print("Batch complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--lookup", required=True)
    parser.add_argument("--silver", default="s3a://nyc-silver/trips")
    parser.add_argument("--quarantine", default="s3a://nyc-quarantine/invalid_trips")
    parser.add_argument("--year", type=int, default=None,
                        help="Filter to this pickup_year (omit to keep all)")
    parser.add_argument("--month", type=int, default=None,
                        help="Filter to this pickup_month (omit to keep all)")
    args = parser.parse_args()
    run_batch(args.input, args.lookup, args.silver, args.quarantine,
              expected_year=args.year, expected_month=args.month)
