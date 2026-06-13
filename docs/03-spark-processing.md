# 3. Xử Lý Dữ Liệu với Apache Spark

## 3.1 Tổng quan

Apache Spark 3.5.1 là engine xử lý chính của pipeline, đảm nhận việc:
- Đọc dữ liệu thô (Parquet từ MinIO S3 hoặc Kafka)
- **Enrichment**: Cast kiểu, join zone lookup, tạo derived columns
- **Validation**: 10 rules kiểm tra chất lượng dữ liệu
- **Split**: Tách valid/invalid, ghi vào MinIO S3

Có 2 chế độ xử lý:
1. **Spark Batch** (`spark_local_batch.py`): local[*], một lần cho backfill lịch sử
2. **Spark Streaming** (`spark_stream_taxi_events.py`): Kafka consumer, micro-batch

Ngoài ra còn có:
- `spark_quality_report.py` — Quality report không cần Spark runtime (dùng PyArrow)
- `spark_batch_backfill.py` — Placeholder (legacy)
- `kafka_stream_processor.py` — Python-only processor (không Spark, dùng Kafka-Python + Pandas)

---

## 3.2 Spark Batch Processor

**File**: `jobs/spark_local_batch.py`

### 3.2.1 Usage

### Kubernetes (Skaffold/Airflow) ⭐

Airflow DAG `nyc_e2e_pipeline` tự động submit Spark batch qua `KubernetesPodOperator`:

```python
# Airflow DAG task — tự động chạy mỗi tháng
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    cmds=["/opt/spark/bin/spark-submit"],
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,...",
        "--conf", "spark.jars.ivy=/opt/project/.ivy2",
        "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
        "/opt/project/jobs/spark_local_batch.py",
        "--input", "s3a://nyc-raw/yellow_taxi/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/yellow_tripdata_{{ logical_date.strftime('%Y') }}-{{ logical_date.strftime('%m') }}.parquet",
        "--lookup", "s3a://nyc-lookup/taxi_zone_lookup.csv",
    ],
    env_vars=[
        {"MINIO_ENDPOINT": "http://svc-minio:9000"},
        {"MINIO_ACCESS_KEY": "minio"},
        {"MINIO_SECRET_KEY": "minio123"},
    ],
)
```

**Đặc điểm trong Airflow:**
- Dùng `logical_date` (Jinja template) để lấy year/month từ schedule
- Ivy cache tại `/opt/project/.ivy2/` (shared PVC) — tránh re-download mỗi lần
- S3A packages qua `--packages` CLI
- Mount PVC project-files vào `/opt/project`

### Docker Compose (Legacy)

```bash
make spark-batch
# hoặc:
docker run --rm \
  --network nyc_new_default \
  -v $(pwd):/opt/project -w /opt/project \
  --entrypoint /opt/spark/bin/spark-submit \
  apache/spark:3.5.1 \
  --master local[*] \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,..." \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/project/jobs/spark_local_batch.py \
  --input "s3a://nyc-raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet" \
  --lookup "s3a://nyc-lookup/taxi_zone_lookup.csv"
```

### 3.2.2 Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--input` | required | Path to input Parquet file (S3 hoặc local) |
| `--lookup` | required | Path to taxi_zone_lookup.csv |
| `--silver` | `s3a://nyc-silver/trips` | Output path cho valid trips |
| `--quarantine` | `s3a://nyc-quarantine/invalid_trips` | Output path cho invalid trips |

### 3.2.3 Spark Config (từ SparkSession)

```python
spark = SparkSession.builder \
    .appName("LocalBatchEnriched") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)      # MinIO S3 endpoint
    .config("spark.hadoop.fs.s3a.access.key", access_key)   # minio
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)   # minio123
    .config("spark.hadoop.fs.s3a.path.style.access", "true") # MinIO cần path-style
    .getOrCreate()
```

**K8s mode (Airflow) bổ sung:**
```bash
--conf spark.jars.ivy=/opt/project/.ivy2
--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2
--conf spark.scheduler.mode=FAIR
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
```

### 3.2.4 Luồng xử lý chi tiết

#### Bước 1: Đọc dữ liệu
```python
raw = spark.read.parquet(input_path)           # Raw Parquet
zones_raw = spark.read.option("header", "true").csv(lookup_path)  # CSV zone lookup
```

#### Bước 2: Enrichment
```python
enriched = raw.select(
    col("VendorID").cast("int").alias("vendor_id"),
    to_timestamp("tpep_pickup_datetime").alias("pickup_ts"),
    to_timestamp("tpep_dropoff_datetime").alias("dropoff_ts"),
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    col("PULocationID").cast("int").alias("pickup_location_id"),
    col("DOLocationID").cast("int").alias("dropoff_location_id"),
    col("payment_type").cast("int"),
    col("fare_amount").cast("double"),
    col("total_amount").cast("double"),
    # ... các trường khác
)
```

**Derived columns được thêm vào:**
- `trip_id`: xxhash64 của `pickup_ts|pickup_location_id|dropoff_location_id`
- `source_file`: tên file Parquet gốc
- `event_ts`, `ingestion_ts`: thời gian xử lý
- `pickup_date`, `pickup_hour`, `pickup_year`, `pickup_month`: partition columns
- `pickup_borough/zone/service_zone` và `dropoff_*`: từ zone lookup join

#### Bước 3: Zone Lookup Join
```python
# Chuẩn bị 2 lookup tables
pickup_zones = zones.select(
    col("location_id").alias("pickup_location_id"),
    col("borough").alias("pickup_borough"),
    col("zone").alias("pickup_zone"),
    col("service_zone").alias("pickup_service_zone"),
)
dropoff_zones = zones.select(
    col("location_id").alias("dropoff_location_id"),
    col("borough").alias("dropoff_borough"),
    col("zone").alias("dropoff_zone"),
    col("service_zone").alias("dropoff_service_zone"),
)

# Left join
enriched = enriched.join(pickup_zones, on="pickup_location_id", how="left")
enriched = enriched.join(dropoff_zones, on="dropoff_location_id", how="left")
```

#### Bước 4: Validation (10 rules)
```python
error_array = array(
    when(col("pickup_ts").isNull(), lit("pickup_datetime_null_or_invalid")),
    when(col("dropoff_ts").isNull(), lit("dropoff_datetime_null_or_invalid")),
    when(col("dropoff_ts") <= col("pickup_ts"), lit("invalid_trip_duration")),
    when(col("trip_distance") <= 0, lit("non_positive_trip_distance")),
    when(col("fare_amount") < 0, lit("negative_fare_amount")),
    when(col("total_amount") < col("fare_amount"), lit("total_amount_less_than_fare")),
    when(...passenger_count out of 1-6..., lit("invalid_passenger_count")),
    when(...payment_type not 1-6..., lit("payment_type_out_of_range")),
    when(...pickup_borough null..., lit("unknown_pickup_location")),
    when(...dropoff_borough null..., lit("unknown_dropoff_location")),
)

validated = enriched
    .withColumn("validation_error_candidates", error_array)
    .withColumn("validation_errors",
                expr("filter(validation_error_candidates, x -> x is not null)"))
    .withColumn("is_valid", size(col("validation_errors")) == lit(0))
    .withColumn("quarantine_ts", current_timestamp())
```

#### Bước 5: Split và ghi
```python
valid = validated.filter(col("is_valid"))
invalid = validated.filter(~col("is_valid"))

# Valid: partitioned by pickup_year, pickup_month
valid.select(silver_columns) \
    .write.partitionBy("pickup_year", "pickup_month") \
    .mode("append") \
    .parquet(silver_path)

# Invalid: non-partitioned + kèm validation_errors
invalid.select(silver_columns + ["validation_errors", "quarantine_ts"]) \
    .write.mode("append") \
    .parquet(quarantine_path)
```

> **Lưu ý**: Luôn dùng `mode("append")` — không dùng `overwrite` 
> để tránh mất dữ liệu do `partitionOverwriteMode=dynamic`.

### 3.2.5 Columns đầu ra (Silver)

| Column | Type | Description |
|--------|------|-------------|
| `trip_id` | BIGINT | xxhash64(pickup_ts, pickup_loc, dropoff_loc) |
| `source_file` | VARCHAR | Tên file Parquet gốc |
| `vendor_id` | INTEGER | 1=Creative Mobile, 2=VeriFone |
| `pickup_ts` | TIMESTAMP | Thời gian đón khách |
| `dropoff_ts` | TIMESTAMP | Thời gian trả khách |
| `passenger_count` | INTEGER | 1-6 |
| `trip_distance` | DOUBLE | Dặm |
| `rate_code_id` | INTEGER | 1=Standard, 2=JFK... |
| `pickup_location_id` | INTEGER | Zone ID (1-265) |
| `dropoff_location_id` | INTEGER | Zone ID |
| `payment_type` | INTEGER | 1=Credit, 2=Cash... |
| `fare_amount` | DOUBLE | Giá cước |
| `extra` | DOUBLE | Phụ phí |
| `mta_tax` | DOUBLE | Thuế MTA ($0.50) |
| `tip_amount` | DOUBLE | Tiền tip |
| `tolls_amount` | DOUBLE | Phí cầu đường |
| `improvement_surcharge` | DOUBLE | Phụ phí ($0.30) |
| `total_amount` | DOUBLE | Tổng tiền |
| `pickup_borough/zone/service_zone` | VARCHAR | Thông tin vùng đón |
| `dropoff_borough/zone/service_zone` | VARCHAR | Thông tin vùng trả |
| `pickup_year/month/date/hour` | INT/DATE | Partition + temporal columns |
| `event_ts`, `ingestion_ts` | TIMESTAMP | Metadata timestamps |

---

## 3.3 Spark Streaming Processor

**File**: `jobs/spark_stream_taxi_events.py`

### 3.3.1 Usage

### Kubernetes (Skaffold/Airflow) ⭐

Airflow DAG `nyc_e2e_pipeline` tự động submit Spark streaming với `--trigger-available-now`:

```python
KubernetesPodOperator(
    image="apache/spark:3.5.1",
    arguments=[
        "--master", "local[*]",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,...",
        "/opt/project/jobs/spark_stream_taxi_events.py",
        "--bootstrap-server", "svc-kafka:9092",
        "--topic", "taxi.trip.events",
        "--trigger-available-now",
        "--checkpoint-path", "s3a://nyc-silver/checkpoints/spark_stream_taxi_events/...",
    ],
    env_vars=[{"MINIO_ENDPOINT": "http://svc-minio:9000"}, ...],
)
```

**Lưu ý K8s:**
- Kafka bootstrap: `svc-kafka:9092` (⚠️ prefix `svc-`)
- Checkpoint trên S3: `s3a://nyc-silver/checkpoints/...`
- Cần `spark-sql-kafka-0-10` package

### Docker Compose (Legacy)

```bash
make spark-streaming
# hoặc:
TOPIC=taxi.trip.events bash scripts/start_streaming_job_docker.sh
```

### 3.3.2 Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--bootstrap-server` | localhost:29092 | Kafka bootstrap servers |
| `--topic` | taxi.trip.events | Kafka input topic |
| `--lookup-path` | s3a://nyc-lookup/taxi_zone_lookup.csv | Zone lookup |
| `--silver-path` | s3a://nyc-silver/trips | Output silver |
| `--quarantine-path` | s3a://nyc-quarantine/invalid_trips | Output quarantine |
| `--checkpoint-path` | data/checkpoints/... | Streaming checkpoint |
| `--trigger-available-now` | false | Micro-batch one-shot mode |

### 3.3.3 Kafka Event Schema

```python
EVENT_SCHEMA = StructType([
    StructField("event_id",           StringType(), True),
    StructField("event_timestamp",    StringType(), True),
    StructField("source_file",        StringType(), True),
    StructField("vendor_id",          IntegerType(), True),
    StructField("pickup_datetime",    StringType(), True),
    StructField("dropoff_datetime",   StringType(), True),
    StructField("passenger_count",    IntegerType(), True),
    StructField("trip_distance",      DoubleType(), True),
    StructField("rate_code_id",       IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id",IntegerType(), True),
    StructField("payment_type",       IntegerType(), True),
    StructField("fare_amount",        DoubleType(), True),
    StructField("extra",              DoubleType(), True),
    StructField("mta_tax",            DoubleType(), True),
    StructField("tip_amount",         DoubleType(), True),
    StructField("tolls_amount",       DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount",       DoubleType(), True),
])
```

### 3.3.4 Luồng xử lý

```python
# 1. Read stream từ Kafka
raw = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", args.bootstrap_server)
    .option("subscribe", args.topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

# 2. Parse JSON value
parsed = raw.select(
    col("value").cast("string").alias("raw_value"),
    from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    col("topic"), col("partition"), col("offset"), col("timestamp"),
)

# 3. Enrich (giống batch)
# 4. Validate (giống batch)
# 5. foreachBatch micro-batch
validated.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", args.checkpoint_path)
    .trigger(availableNow=True)  # one-shot mode
    .start()
```

### 3.3.5 foreachBatch Writer

```python
def write_batch(batch_df, batch_id):
    batch_df.persist()
    
    valid_df = batch_df.filter(col("is_valid") == True)
    invalid_df = batch_df.filter(col("is_valid") == False)
    
    if not valid_df.rdd.isEmpty():
        valid_df.select(silver_columns) \
            .write.mode("append") \
            .partitionBy("pickup_year", "pickup_month") \
            .parquet(args.silver_path)
    
    if not invalid_df.rdd.isEmpty():
        invalid_df.write.mode("append") \
            .parquet(args.quarantine_path)
    
    batch_df.unpersist()
```

---

## 3.4 Python-Only Kafka Processor

**File**: `jobs/kafka_stream_processor.py`

Alternative processor sử dụng **Kafka-Python + Pandas + PyArrow** (không cần Spark).

### 3.4.1 Đặc điểm

- **Không cần Spark runtime** — lightweight, chạy trên Python thuần
- **Poll-based loop**: consumer.poll() với max-empty-polls tự động thoát
- **Write**: PyArrow `write_to_dataset()` cho valid, `write_table()` cho invalid
- **Tốc độ**: Chậm hơn Spark, phù hợp development/testing

### 3.4.2 Usage
```bash
python3 jobs/kafka_stream_processor.py \
  --bootstrap-server localhost:29092 \
  --topic taxi.trip.events \
  --lookup-path data/lookup/taxi_zone_lookup.csv \
  --silver-path data/silver/trips \
  --quarantine-path data/quarantine/invalid_trips
```

---

## 3.5 Quality Report

**File**: `jobs/spark_quality_report.py`

Sinh báo cáo chất lượng dữ liệu bằng PyArrow (không cần Spark).

```bash
make quality-report
# hoặc
python3 jobs/spark_quality_report.py \
  --silver-path data/silver/trips \
  --quarantine-path data/quarantine/invalid_trips \
  --output reports/data_quality_report.md
```

**Output example:**
```markdown
# Data Quality Report

Generated at: 2026-06-12T10:30:00.000000+00:00

- Total records processed: **9,554,778**
- Valid records: **8,480,408**
- Invalid records: **1,074,370**
- Invalid percentage: **11.24%**
```

---

## 3.6 Lưu ý quan trọng

### S3A Package
```bash
# BẮT BUỘC: dùng --packages trên spark-submit CLI
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

# KHÔNG dùng spark.jars.packages trong SparkSession config
```

### Ivy Cache
```bash
# Chia sẻ Ivy cache trên PVC để tránh re-download mỗi lần
--conf spark.jars.ivy=/opt/project/.ivy2/
# Permissions
chmod -R 777 /opt/project/.ivy2/
```

### MinIO S3 Commit Fix
```python
# BẮT BUỘC: MinIO không hỗ trợ atomic S3 rename
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
```

### Output Mode
```python
# Luôn dùng mode("append") — không overwrite
# partitionOverwriteMode=dynamic không hoạt động đúng với MinIO
```

### Partition Columns
```python
# Valid trips: partitioned by pickup_year, pickup_month
# Columns phải là LAST trong select
```

### Streaming Checkpoint
```python
# Checkpoint trên S3 cho K8s mode
--checkpoint-path "s3a://nyc-silver/checkpoints/spark_stream_taxi_events/..."
# Checkpoint local cho Docker mode
--checkpoint-path "data/checkpoints/spark_stream_taxi_events"
```
