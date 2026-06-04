# NYC Taxi Data Pipeline Challenge

## 1. Challenge Overview

Build a batch data pipeline using NYC Taxi trip data.

Students need to download public taxi trip records, store them in a local data lake, process them with Spark, validate the data, transform it into analytics tables, and answer business questions using SQL or a dashboard.

The goal is to practice a realistic data engineering workflow:

```text
Raw Data → Data Lake → Spark Processing → Data Quality → dbt Models → Analytics
```

---

## 2. Dataset Download

Use the official NYC Taxi & Limousine Commission trip record data.

Dataset page:

```text
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

The NYC TLC trip records include pickup/drop-off times, pickup/drop-off locations, trip distances, fares, rate types, payment types, and passenger counts.

### Required files

Students should download at least **3 months** of Yellow Taxi data.

Example files:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet
```

Also download the taxi zone lookup file:

```text
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Data dictionary:

```text
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
```

The AWS Open Data Registry also lists the NYC TLC trip record dataset and notes that anonymous downloads are available from the dataset documentation page.

---

## 3. Suggested Tech Stack

### Option A — Beginner

Use this if students are new to data engineering.

| Layer          | Tool                            |
| -------------- | ------------------------------- |
| Runtime        | Docker Compose                  |
| Storage        | Local folder or MinIO           |
| Processing     | PySpark                         |
| Database       | PostgreSQL                      |
| Transformation | dbt                             |
| Data Quality   | dbt tests or Great Expectations |
| Dashboard      | Metabase or Superset            |

### Option B — Intermediate

Use this if students already know Docker and SQL.

| Layer          | Tool                  |
| -------------- | --------------------- |
| Runtime        | Docker Compose        |
| Orchestration  | Airflow               |
| Storage        | MinIO                 |
| Processing     | Spark                 |
| Table Format   | Parquet or Delta Lake |
| Query Engine   | Trino                 |
| Transformation | dbt                   |
| Data Quality   | Great Expectations    |
| Dashboard      | Superset              |

### Option C — Advanced / Bonus

Use this if students want to try streaming.

| Layer             | Tool                       |
| ----------------- | -------------------------- |
| Streaming         | Kafka                      |
| CDC               | Debezium                   |
| Stream Processing | Spark Structured Streaming |
| Source DB         | PostgreSQL                 |
| Data Lake         | MinIO                      |
| Analytics         | Trino / dbt / Superset     |

---

## 4. Required Pipeline

Students must build this flow:

```text
NYC Taxi Parquet Files
        ↓
Raw Zone
        ↓
Spark Cleaning Job
        ↓
Silver Zone
        ↓
dbt Models
        ↓
Analytics Tables
        ↓
SQL Queries / Dashboard
```

---

## 5. Data Lake Structure

Use this simple folder layout:

```text
data/
├── raw/
│   └── yellow_taxi/
├── lookup/
│   └── taxi_zone_lookup.csv
├── silver/
│   └── trips/
├── gold/
│   ├── revenue_by_day/
│   ├── revenue_by_zone/
│   └── trips_by_hour/
└── quarantine/
    └── invalid_trips/
```

---

## 6. Batch Processing Requirements

### 6.1 Raw Layer

Store downloaded files without modification.

Example:

```text
data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet
data/raw/yellow_taxi/year=2024/month=02/yellow_tripdata_2024-02.parquet
data/raw/yellow_taxi/year=2024/month=03/yellow_tripdata_2024-03.parquet
```

### 6.2 Silver Layer

Use Spark to clean and standardize the data.

Required transformations:

* Rename columns to lowercase.
* Add `trip_id`.
* Add `ingestion_ts`.
* Add `source_file`.
* Extract `pickup_date`.
* Extract `pickup_hour`.
* Join with `taxi_zone_lookup.csv`.
* Remove invalid records.
* Write clean data to Parquet.

Suggested output:

```text
data/silver/trips/pickup_year=2024/pickup_month=01/
```

### 6.3 Invalid Records

Invalid records should not be deleted silently.

Write them to:

```text
data/quarantine/invalid_trips/
```

---

## 7. Data Quality Rules

Students must check these rules:

| Check             | Rule                                         |
| ----------------- | -------------------------------------------- |
| Pickup time       | Must not be null                             |
| Drop-off time     | Must not be null                             |
| Trip duration     | Drop-off time must be after pickup time      |
| Trip distance     | Must be greater than 0                       |
| Fare amount       | Must be greater than or equal to 0           |
| Total amount      | Must be greater than or equal to fare amount |
| Passenger count   | Must be between 1 and 6                      |
| Pickup location   | Must exist in taxi zone lookup               |
| Drop-off location | Must exist in taxi zone lookup               |
| Duplicate trip    | `trip_id` should be unique                   |

---

## 8. dbt Models

Build simple dbt models from the cleaned taxi data.

### 8.1 Staging Models

| Model     | Description               |
| --------- | ------------------------- |
| stg_trips | Cleaned taxi trip records |
| stg_zones | Taxi zone lookup table    |

### 8.2 Dimension Tables

| Model            | Description            |
| ---------------- | ---------------------- |
| dim_zone         | Taxi zone dimension    |
| dim_date         | Date dimension         |
| dim_payment_type | Payment type dimension |

### 8.3 Fact Table

| Model      | Grain                 |
| ---------- | --------------------- |
| fact_trips | One row per taxi trip |

### 8.4 Mart Tables

| Model                     | Description                             |
| ------------------------- | --------------------------------------- |
| mart_revenue_by_day       | Daily revenue summary                   |
| mart_revenue_by_zone      | Revenue by pickup/drop-off zone         |
| mart_trips_by_hour        | Trip count by hour                      |
| mart_payment_type_summary | Revenue and tip summary by payment type |

---

## 9. Analytics Questions

Students must answer at least **8 questions**.

Suggested questions:

1. What is total revenue by day?
2. What is total trip count by day?
3. Which pickup zones generate the most revenue?
4. Which drop-off zones are most popular?
5. What is the average fare by pickup borough?
6. What is the average trip distance by hour?
7. Which payment type has the highest average tip?
8. What are the busiest pickup hours?
9. What percentage of records were invalid?
10. Which routes have the highest average fare?

---

## 10. Optional Advanced Tasks

Students can choose one or more:

### Option 1 — Add Airflow

Create an Airflow DAG with these tasks:

```text
download_data
    ↓
upload_to_raw
    ↓
spark_clean_trips
    ↓
run_data_quality_checks
    ↓
run_dbt_models
```

### Option 2 — Add Trino

Use Trino to query data directly from the lake.

Example:

```sql
SELECT
    pickup_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
FROM silver.trips
GROUP BY pickup_date
ORDER BY pickup_date;
```

### Option 3 — Add Kafka Streaming

Create a small generator that reads historical taxi data and publishes fake real-time trip events to Kafka.

Topic:

```text
taxi.trip.events
```

Required event fields:

| Field               | Description     |
| ------------------- | --------------- |
| event_id            | Unique event ID |
| event_timestamp     | Time of event   |
| taxi_id             | Taxi ID         |
| pickup_location_id  | Pickup zone     |
| dropoff_location_id | Drop-off zone   |
| trip_distance       | Trip distance   |
| fare_amount         | Fare amount     |
| total_amount        | Total amount    |
| payment_type        | Payment type    |

### Option 4 — Add Debezium CDC

Create PostgreSQL tables:

```text
drivers
vehicles
payments
```

Use Debezium to capture changes into Kafka.

---

## 11. Configuration Example

```yaml
project_name: "nyc-taxi-pipeline"
taxi_type: "yellow"
start_month: "2024-01"
end_month: "2024-03"

storage:
  type: "local"
  raw_path: "data/raw"
  silver_path: "data/silver"
  gold_path: "data/gold"
  quarantine_path: "data/quarantine"

processing:
  engine: "spark"
  output_format: "parquet"
  partition_by:
    - pickup_year
    - pickup_month

quality:
  enable_checks: true
  quarantine_invalid_records: true

dbt:
  enabled: true
  target_database: "postgresql"
  schema: "analytics"

optional:
  airflow: false
  trino: false
  kafka_streaming: false
  debezium_cdc: false
```

---

## 12. Deliverables

Students must submit:

1. **Source code**

   * Spark job
   * dbt project
   * SQL queries
   * Docker Compose file if used

2. **Data pipeline output**

   * Raw files
   * Silver cleaned data
   * Gold/mart tables
   * Quarantine invalid records

3. **Data quality report**

   * Number of input records
   * Number of valid records
   * Number of invalid records
   * Invalid record percentage
   * Duplicate count
   * Null count for important columns

4. **Analytics result**

   * SQL answers for at least 8 questions
   * Optional dashboard screenshot

5. **Short write-up**

   * Tech stack used
   * Pipeline design
   * Problems found in the data
   * What could be improved for production

---

## 13. Evaluation Criteria

| Area                         |   Score |
| ---------------------------- | ------: |
| Dataset downloaded correctly |      10 |
| Raw/silver/gold structure    |      15 |
| Spark transformation         |      20 |
| Data quality checks          |      15 |
| dbt models                   |      15 |
| Analytics SQL/dashboard      |      15 |
| Documentation                |      10 |
| **Total**                    | **100** |

---

## 14. Notes for Students

Start simple.

Recommended order:

```text
1. Download 3 months of Yellow Taxi data
2. Read the files with Spark
3. Clean invalid records
4. Join with taxi zone lookup
5. Write silver Parquet data
6. Build dbt models
7. Answer analytics questions
8. Add Airflow, Trino, Kafka, or Debezium only after the batch pipeline works
```

Do not start with Kafka or Debezium first. Build the batch pipeline first, then add advanced components later.
