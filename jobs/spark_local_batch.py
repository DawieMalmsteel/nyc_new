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
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def run_batch(input_path, lookup_path, silver_path, quarantine_path):
    print(f"Starting enriched batch")
    print(f"  input:      {input_path}")
    print(f"  lookup:     {lookup_path}")
    print(f"  silver:     {silver_path}")
    print(f"  quarantine: {quarantine_path}")

    spark = SparkSession.builder \
        .appName("LocalBatchEnriched") \
        .master("local[*]") \
        .getOrCreate()

    # Enables dynamic partition overwrite (only touch matching partitions)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # --- 1. Read raw parquet ---
    raw = spark.read.parquet(input_path)

    zones_raw = spark.read.option("header", "true").csv(lookup_path)
    zones = zones_raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
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

    # Add trip_id (hash of pickup_ts + pickup_loc + dropoff_loc) + source_file
    input_filename = input_path.split("/")[-1]
    enriched = enriched \
        .withColumn("trip_id",
            F.xxhash64(F.concat_ws("|",
                F.col("pickup_ts").cast("string"),
                F.col("pickup_location_id").cast("string"),
                F.col("dropoff_location_id").cast("string")
            ))) \
        .withColumn("source_file", F.lit(input_filename))
    # Add metadata columns
    enriched = enriched \
        .withColumn("event_ts", F.current_timestamp()) \
        .withColumn("ingestion_ts", F.current_timestamp()) \
        .withColumn("pickup_date", F.to_date(F.col("pickup_ts"))) \
        .withColumn("pickup_hour", F.hour(F.col("pickup_ts"))) \
        .withColumn("pickup_year", F.year(F.col("pickup_ts"))) \
        .withColumn("pickup_month", F.month(F.col("pickup_ts")))

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
            .mode("overwrite") \
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
    parser.add_argument("--silver", default="/opt/project/data/silver/trips")
    parser.add_argument("--quarantine", default="/opt/project/data/quarantine/invalid_trips")
    args = parser.parse_args()
    run_batch(args.input, args.lookup, args.silver, args.quarantine)
