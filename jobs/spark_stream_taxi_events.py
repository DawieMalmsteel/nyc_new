#!/usr/bin/env python3
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    current_timestamp,
    expr,
    from_json,
    hour,
    lit,
    month,
    size,
    to_date,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("rate_code_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pickup_location_id", IntegerType(), True),
        StructField("dropoff_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
    ]
)


def build_spark(s3_mode: bool = False) -> SparkSession:
    builder = SparkSession.builder.appName("nyc-taxi-kafka-stream") \
        .config("spark.sql.shuffle.partitions", "4")
    if s3_mode:
        endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minio")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minio123")
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    parser.add_argument("--topic", default="taxi.trip.events")
    parser.add_argument("--lookup-path", default="data/lookup/taxi_zone_lookup.csv")
    parser.add_argument("--silver-path", default="data/silver/trips")
    parser.add_argument("--quarantine-path", default="data/quarantine/invalid_trips")
    parser.add_argument("--checkpoint-path", default="data/checkpoints/spark_stream_taxi_events")
    parser.add_argument("--trigger-available-now", action="store_true")
    parser.add_argument("--s3", action="store_true", help="Use MinIO S3-compatible storage")
    args = parser.parse_args()

    spark = build_spark(s3_mode=args.s3)

    zones = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(args.lookup_path)
        .select(
            col("LocationID").cast("int").alias("location_id"),
            col("Borough").alias("borough"),
            col("Zone").alias("zone"),
            col("service_zone").alias("service_zone"),
        )
    )

    pickup_zones = zones.select(
        col("location_id").alias("pickup_zone_id"),
        col("borough").alias("pickup_borough"),
        col("zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone"),
    )

    dropoff_zones = zones.select(
        col("location_id").alias("dropoff_zone_id"),
        col("borough").alias("dropoff_borough"),
        col("zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone"),
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_value"),
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    )

    df = parsed.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
        "event.*",
    )

    df = (
        df.withColumn("pickup_ts", to_timestamp(col("pickup_datetime")))
        .withColumn("dropoff_ts", to_timestamp(col("dropoff_datetime")))
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("pickup_date", to_date(col("pickup_ts")))
        .withColumn("pickup_hour", hour(col("pickup_ts")))
        .withColumn("pickup_year", year(col("pickup_ts")))
        .withColumn("pickup_month", month(col("pickup_ts")))
    )

    df = (
        df.join(
            pickup_zones,
            col("pickup_location_id") == col("pickup_zone_id"),
            "left",
        )
        .drop("pickup_zone_id")
        .join(
            dropoff_zones,
            col("dropoff_location_id") == col("dropoff_zone_id"),
            "left",
        )
        .drop("dropoff_zone_id")
    )

    error_array = array(
        when(col("event_id").isNull(), lit("event_id_null")),
        when(col("pickup_ts").isNull(), lit("pickup_datetime_null_or_invalid")),
        when(col("dropoff_ts").isNull(), lit("dropoff_datetime_null_or_invalid")),
        when(col("dropoff_ts") <= col("pickup_ts"), lit("invalid_trip_duration")),
        when(col("trip_distance") <= lit(0), lit("trip_distance_must_be_gt_0")),
        when(col("fare_amount") < lit(0), lit("fare_amount_must_be_gte_0")),
        when(col("total_amount") < col("fare_amount"), lit("total_amount_must_be_gte_fare_amount")),
        when(
            (col("passenger_count") < lit(1)) | (col("passenger_count") > lit(6)),
            lit("passenger_count_out_of_range"),
        ),
        when(col("pickup_zone").isNull(), lit("pickup_location_not_found")),
        when(col("dropoff_zone").isNull(), lit("dropoff_location_not_found")),
    )

    validated = (
        df.withColumn("validation_error_candidates", error_array)
        .withColumn("validation_errors", expr("filter(validation_error_candidates, x -> x is not null)"))
        .drop("validation_error_candidates")
        .withColumn("is_valid", size(col("validation_errors")) == lit(0))
        .withColumn("quarantine_ts", current_timestamp())
    )

    def write_batch(batch_df, batch_id: int):
        _ = batch_id
        batch_df.persist()

        valid_df = batch_df.filter(col("is_valid") == lit(True)).drop("is_valid")
        invalid_df = batch_df.filter(col("is_valid") == lit(False)).drop("is_valid")

        if not valid_df.rdd.isEmpty():
            (
                valid_df.write.mode("append")
                .partitionBy("pickup_year", "pickup_month")
                .parquet(args.silver_path)
            )

        if not invalid_df.rdd.isEmpty():
            invalid_df.write.mode("append").parquet(args.quarantine_path)

        batch_df.unpersist()

    writer = (
        validated.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint_path)
    )

    if args.trigger_available_now:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
