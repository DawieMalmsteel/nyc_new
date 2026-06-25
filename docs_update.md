# NYC Taxi Pipeline — Updated Architecture with Data Quality Monitoring

> Thiết kế hệ thống monitoring & quality gate. Không sửa code cũ.

---

## Hai luồng song song

```mermaid
flowchart LR
    subgraph MAIN["MAIN FLOW — pipeline chính (chạy monthly)"]
        direction LR
        M1["spark_batch"] --> M2["trino_bootstrap"]
        M0["spark_streaming"] --> M2
        M2 --> M3["dbt_build"]
        M3 --> M4["gold_export<br/>→ MinIO"]
        M3 --> M5["materialize<br/>→ Postgres"]
        M5 --> M6["superset_bootstrap"]
        M6 --> M7["analytics_check"]
    end

    subgraph MONITOR["MONITOR FLOW — giám sát song song (@every 5min, read-only)"]
        direction LR
        N1["check_silver<br/>row count, null, dist"]
        N0["check_streaming<br/>consumer lag, offset"]
        N2["check_gold<br/>30 tables, match"]
        N3["check_postgres<br/>pg = gold"]
        N4["check_superset<br/>charts OK?"]
        N5["check_freshness<br/>data stale?"]
        N_cdc1["check_pg_cdc<br/>WAL size, slot health"]
        N_cdc2["check_debezium<br/>connector status"]
        N_cdc3["check_kafka<br/>broker health, lag"]
    end

    M1 -.->|"quan sát"| N1
    M0 -.->|"quan sát"| N0
    M0 -.->|"CDC chain"| N_cdc1
    M0 -.->|"CDC chain"| N_cdc2
    M0 -.->|"CDC chain"| N_cdc3
    M4 -.->|"quan sát"| N2
    M5 -.->|"quan sát"| N3
    M6 -.->|"quan sát"| N4
    M7 -.->|"quan sát"| N5

    N0 & N1 & N2 & N3 & N4 & N5 & N_cdc1 & N_cdc2 & N_cdc3 --> ALERT["🚨 Slack + Email<br/>nếu FAIL"]

    style MAIN fill:#1a1a2e,stroke:#555,color:#ddd
    style MONITOR fill:#16213e,stroke:#0f3460,color:#ddd
    style ALERT fill:#c00,stroke:#333,color:#fff
```

> **MAIN FLOW**: Chạy pipeline như cũ, không thay đổi gì.
> **MONITOR FLOW**: DAG riêng, chạy mỗi 5 phút, chỉ SELECT không ghi. Quan sát output từng node + CDC chain (Postgres CDC WAL, Debezium status, Kafka broker). Nếu phát hiện lỗi → Slack + Email.
> **Không can thiệp**: Monitor fail không ảnh hưởng MAIN. MAIN fail không ảnh hưởng Monitor.
> **Đường đứt nét** (`-.->`) = observation only, không phải dependency.
>
> **3 check CDC chain mới**: `check_pg_cdc` (WAL size + replication slot), `check_debezium` (connector RUNNING?), `check_kafka` (broker health + consumer lag). CDC chain chết → phát hiện trong < 5 phút thay vì 35 ngày (freshness check).

### Xử lý khi Spark batch timeout (data nhiều năm)

| Tình huống | Cách xử lý | Ai lo |
|---|---|---|
| **Lần đầu full load** (5 năm, ~180M dòng) → `local[*]` timeout | **Check resource pod trước**: nếu RAM > 8GB → chạy full. Nếu không → auto-split từng tháng sequential. Tăng `execution_timeout` lên 120 phút | Spark tự check `psutil.virtual_memory()` trước khi chạy |
| **Từ lần 2 trở đi** (thêm 1 tháng, ~3M dòng) | `--incremental` có sẵn trong Spark code, chỉ đọc partition mới → 3-4 phút | Không cần sửa gì |
| **Data tăng đột biến** (tháng cao điểm 6M dòng) | `local[*]` vẫn xử lý được 6M dòng ~5-6 phút | Không cần sửa |
| **Spark OOM** | Tăng `spark.driver.memory`, thêm `spark.sql.shuffle.partitions=200` | Spark submit args |
| **Spark OOM kill giữa chừng** → silver có file dở dang → incremental skip luôn tháng đó → mất data vĩnh viễn | **Ghi vào thư mục tạm** (`silver/_tmp/month=06`) → ghi xong mới move vào `silver/trips/month=06`. Nếu crash → thư mục `_tmp` bị bỏ lại, không ảnh hưởng data cũ. Retry → xóa `_tmp` → ghi lại từ đầu | Spark code + verify_gate check row count tháng mới |
| **Trino OOM sau khi Spark xong** | Partition gold export theo `pickup_year`, giới hạn concurrent query = 5 | DAG + Trino config |

```mermaid
flowchart TD
    subgraph STRATEGY["Spark Batch Scalability + Crash Recovery"]
        direction TB
        Q{"Silver có data chưa?"}
        Q -->|"Có (incremental)"| INC["Chỉ đọc partition mới<br/>3M dòng → 3-4 phút"]
        Q -->|"Không (lần đầu)"| CHECK_RAM{"Pod RAM > 8GB?"}
        CHECK_RAM -->|"Có"| ONESHOT["Chạy full 1 lần<br/>timeout=120 phút"]
        CHECK_RAM -->|"Không"| SPLIT["Auto-split từng tháng<br/>sequential: month=01 → 02 → ..."]
        SPLIT --> M1["month=01"] --> M2["month=02"] --> M3["..."] --> M12["month=12"]
        M12 --> NEXT_YEAR["Sang năm tiếp theo..."]
        NEXT_YEAR --> DONE["Done — từ lần sau incremental"]
        INC --> DONE
        ONESHOT --> DONE
        INC -.->|OOM kill| CRASH["Spark crash giữa chừng"]
        CRASH --> FIX["1. Xóa _tmp dir<br/>2. Retry task<br/>3. verify_silver check row count"]
        FIX --> INC
    end

    style Q fill:#e94,stroke:#333,color:#fff
    style CHECK_RAM fill:#e94,stroke:#333,color:#fff
    style INC fill:#4a4,stroke:#333,color:#fff
    style ONESHOT fill:#4a4,stroke:#333,color:#fff
    style SPLIT fill:#e94,stroke:#333,color:#fff
    style DONE fill:#4a4,stroke:#333,color:#fff
    style CRASH fill:#c00,stroke:#333,color:#fff
    style FIX fill:#e94,stroke:#333,color:#fff
```

---

## 1. Pipeline Nodes — 13 Node, Output Contract

```mermaid
flowchart TD
    subgraph SOURCE["SOURCE"]
        RAW["MinIO nyc-raw<br/>(Parquet)"]
        PG["Postgres CDC<br/>nyc_taxi.public.trips"]
    end

    subgraph INGEST["INGEST (Spark)"]
        SB["spark_batch<br/>local[*]"]
        SS["spark_streaming<br/>Kafka consumer"]
    end

    subgraph STORAGE["MinIO S3"]
        SILVER["nyc-silver/trips<br/>265MB"]
        QUAR["nyc-quarantine<br/>36MB"]
        LOOKUP["nyc-lookup<br/>12KB"]
        GOLD["nyc-gold/*<br/>30 datasets"]
    end

    subgraph CATALOG["CATALOG"]
        TRINO["Trino 435<br/>hive.nyc.*<br/>hive.mart.*<br/>hive.nyc_gold.*"]
    end

    subgraph TRANSFORM["TRANSFORM"]
        DBT["dbt-trino<br/>30 models<br/>54 tests<br/>staging→marts→gold"]
    end

    subgraph EXPORT["EXPORT"]
        GE["gold_export<br/>CTAS 30 tables<br/>→ MinIO"]
        MP["materialize_postgres<br/>COPY 30 tables<br/>→ Postgres"]
    end

    subgraph ANALYTICS["ANALYTICS"]
        PG_ANALYTICS["Postgres Analytics<br/>nyc_analytics.public.*"]
        SUPERSET["Superset<br/>7 datasets<br/>4 charts<br/>1 dashboard"]
    end

    subgraph CDC["CDC PATH"]
        DZ["Debezium 2.5"]
        KAFKA["Kafka<br/>taxi.trip.events"]
        BRIDGE["cdc_bridge"]
    end

    RAW --> SB
    RAW --> PG
    PG --> DZ --> KAFKA --> BRIDGE --> SS
    SB -->|"valid"| SILVER
    SB -->|"invalid"| QUAR
    SS -->|"valid"| SILVER
    SS -->|"invalid"| QUAR
    LOOKUP --> SB
    LOOKUP --> SS
    SILVER --> TRINO
    QUAR --> TRINO
    LOOKUP --> TRINO
    TRINO --> DBT
    DBT -->|"PASS"| GE
    DBT -->|"PASS"| MP
    DBT -.->|"test FAIL → fix + rerun"| ERR1["dbt error"]
    GE --> GOLD
    GE -.->|"OOM/CTAS fail → retry 3x"| ERR2["gold export error"]
    MP --> PG_ANALYTICS
    MP -.->|"INSERT fail → retry"| ERR3["materialize error"]
    GOLD --> TRINO
    PG_ANALYTICS --> SUPERSET
    TRINO --> SUPERSET
    QUAR -.->|"QA: review invalid"| AUDIT["data quality audit"]

    style SB fill:#4a9,stroke:#333,color:#fff
    style SS fill:#4a9,stroke:#333,color:#fff
    style DBT fill:#49a,stroke:#333,color:#fff
    style GE fill:#c94,stroke:#333,color:#fff
    style MP fill:#c94,stroke:#333,color:#fff
    style ERR1 fill:#e44,stroke:#333,color:#fff
    style ERR2 fill:#e44,stroke:#333,color:#fff
    style ERR3 fill:#e44,stroke:#333,color:#fff
    style AUDIT fill:#e94,stroke:#333,color:#fff
    style QUAR fill:#c94,stroke:#333,color:#fff
```

---

## 2. Quality Gate Layer — 5 Gates Block Pipeline

```mermaid
flowchart LR
    subgraph PIPELINE["Main Pipeline (nyc_e2e_pipeline)"]
        BATCH["spark_batch"]
        STREAM["spark_streaming"]
        TB["trino_bootstrap<br/>trigger_rule=one_success"]
        DBT2["dbt_build"]
        GE2["gold_export"]
        MP2["materialize_postgres"]
        SUP2["superset_bootstrap"]
        SSQ["superset_saved_queries"]
        AC["analytics_check"]
    end

    subgraph GATES["QUALITY GATES"]
        V1["verify_silver<br/>✓ row_count > 0<br/>✓ null_ratio = 0<br/>✓ AVG(distance) in range<br/>✓ MAX(date) fresh"]
        V2["verify_gold<br/>✓ 30/30 tables exist<br/>✓ each row_count > 0<br/>✓ match dbt source"]
        V3["verify_postgres<br/>✓ pg rows = gold rows<br/>✓ all tables match"]
        V4["verify_superset<br/>✓ charts render OK<br/>✓ metrics match Trino"]
        V5["verify_freshness<br/>✓ MAX(date) <= 35d<br/>✓ rows in 7d range"]
    end

    subgraph ALERT["ALERT"]
        SLACK["Slack #nyc-alerts"]
        EMAIL["Email on-call"]
        BLOCK["BLOCK downstream"]
    end

    BATCH --> V1
    STREAM --> V1
    V1 -->|PASS| TB
    V1 -->|FAIL| BLOCK
    BLOCK --> SLACK
    BLOCK --> EMAIL

    TB --> DBT2
    DBT2 --> GE2
    GE2 --> V2
    V2 -->|FAIL| BLOCK

    DBT2 --> MP2
    MP2 --> V3
    V3 -->|PASS| SUP2
    V3 -->|FAIL| BLOCK

    SUP2 --> V4
    V4 -->|PASS| SSQ
    V4 -->|FAIL| SLACK

    SSQ --> V5
    V5 -->|PASS| AC
    V5 -->|FAIL| BLOCK

    style V1 fill:#e44,stroke:#333,color:#fff
    style V2 fill:#e44,stroke:#333,color:#fff
    style V3 fill:#e44,stroke:#333,color:#fff
    style V4 fill:#e94,stroke:#333,color:#fff
    style V5 fill:#e44,stroke:#333,color:#fff
    style BLOCK fill:#c00,stroke:#333,color:#fff
    style SLACK fill:#4a4,stroke:#333,color:#fff
    style EMAIL fill:#44a,stroke:#333,color:#fff
```

---

## 3. Full Data Lineage — End to End

```mermaid
flowchart TD
    RAW["MinIO nyc-raw<br/>153MB Parquet"] -->|"read"| SB["spark_batch"]
    PG_CDC["Postgres CDC"] -->|"Debezium"| DZ["debezium"]
    DZ -->|"Kafka"| KAFKA["Kafka<br/>taxi.trip.events"]
    KAFKA -->|"Bridge"| STREAM["spark_streaming"]

    SB -->|"valid → append"| SILVER["nyc-silver<br/>265MB Parquet"]
    SB -->|"invalid → append"| QUAR["nyc-quarantine<br/>36MB Parquet"]
    STREAM -->|"valid → append"| SILVER
    STREAM -->|"invalid → append"| QUAR
    CSV["nyc-lookup<br/>12KB CSV"] -->|"read"| SB
    CSV -->|"read"| STREAM

    SILVER -->|"read S3"| TRINO["Trino<br/>hive.nyc.trips"]
    QUAR -->|"read S3"| TRINO

    TRINO -->|"SELECT"| STG["dbt staging<br/>stg_trips, stg_zones"]
    STG -->|"ref"| MARTS["dbt marts<br/>fact_trips, dim_zone"]
    MARTS -->|"ref"| GOLD_DBT["dbt gold (14 BI)"]

    GOLD_DBT -->|"CTAS"| GOLD_MINIO["nyc-gold<br/>30 datasets Parquet"]
    GOLD_DBT -->|"INSERT"| PG_ANALYTICS["Postgres Analytics<br/>nyc_analytics.public"]

    PG_ANALYTICS -->|"query"| SUPERSET["Superset Dashboard"]
    TRINO -->|"direct query"| SUPERSET

    SB -.->|"fail → retry 3x"| ERR1["spark error"]
    TRINO -.->|"OOM → restart"| ERR2["trino error"]
    GOLD_DBT -.->|"test fail → fix"| ERR3["dbt error"]
    GOLD_MINIO -.->|"partial fail → retry"| ERR4["gold export error"]
    PG_ANALYTICS -.->|"insert fail → retry"| ERR5["materialize error"]
    QUAR -.->|"review"| QA["data quality audit"]

    style RAW fill:#689,stroke:#333,color:#fff
    style SILVER fill:#4a9,stroke:#333,color:#fff
    style QUAR fill:#c94,stroke:#333,color:#fff
    style GOLD_MINIO fill:#e94,stroke:#333,color:#fff
    style TRINO fill:#49a,stroke:#333,color:#fff
    style PG_ANALYTICS fill:#48a,stroke:#333,color:#fff
    style SUPERSET fill:#a4a,stroke:#333,color:#fff
    style ERR1 fill:#e44,stroke:#333,color:#fff
    style ERR2 fill:#e44,stroke:#333,color:#fff
    style ERR3 fill:#e44,stroke:#333,color:#fff
    style ERR4 fill:#e44,stroke:#333,color:#fff
    style ERR5 fill:#e44,stroke:#333,color:#fff
    style QA fill:#e94,stroke:#333,color:#fff
```

---

## 4. Cross-Node Reconciliation

```mermaid
flowchart TD
    subgraph RECONCILE["Reconciliation Checks (block on mismatch)"]
        R1["RECONCILE 1<br/>spark_input_rows == silver_rows + quarantine_rows"]
        R2["RECONCILE 2<br/>silver_rows == mart.fact_trips_rows"]
        R3["RECONCILE 3<br/>mart.* rows == gold_export.* rows"]
        R4["RECONCILE 4<br/>gold.* rows == postgres.* rows"]
    end

    SPARK["spark_batch output"] --> R1
    SILVER2["nyc-silver"] --> R1
    QUAR2["nyc-quarantine"] --> R1

    R1 -->|PASS| R2
    R1 -->|FAIL| BLOCK2["BLOCK pipeline + Slack + Email"]

    SILVER2 --> R2
    MART["mart.fact_trips"] --> R2
    R2 -->|PASS| R3
    R2 -->|FAIL| BLOCK2

    MART --> R3
    GOLD2["nyc-gold/*"] --> R3
    R3 -->|PASS| R4
    R3 -->|FAIL| BLOCK2

    GOLD2 --> R4
    PG2["Postgres analytics"] --> R4
    R4 -->|PASS| OK2["ALL CLEAR - Superset safe to display"]
    R4 -->|FAIL| BLOCK2

    style BLOCK2 fill:#c00,stroke:#333,color:#fff
    style OK2 fill:#4a4,stroke:#333,color:#fff
```

---

## 5. Failure Mode → Detection → Response

```mermaid
flowchart LR
    subgraph FAILURES["Failure Scenarios"]
        F1["MinIO wrong bucket<br/>Spark reads 0 rows"]
        F2["Spark type cast bug<br/>fare_amount all null"]
        F3["Spark crash restart<br/>duplicate rows"]
        F4["Trino OOM<br/>gold export partial"]
        F5["materialize fail<br/>Postgres empty"]
        F6["Superset cache stale<br/>shows old data"]
        F7["NYC TLC no publish<br/>no new data"]
        F8["Kafka consumer lag<br/>CDC delayed"]
        F9["S3 capacity spike<br/>nearly full"]
    end

    subgraph DETECT["Detection"]
        D1["verify_silver<br/>row_count = 0"]
        D2["verify_silver<br/>null_ratio > 0"]
        D3["verify_silver<br/>row_count > expected"]
        D4["verify_gold<br/>tables < 30"]
        D5["verify_postgres<br/>pg_rows != gold_rows"]
        D6["verify_superset<br/>chart data stale"]
        D7["verify_freshness<br/>max_date > 35d"]
        D8["check_kafka_health<br/>consumer_lag > 1000"]
        D9["check_minio_health<br/>size > 2x avg"]
    end

    subgraph RESPONSE["Response"]
        R1["BLOCK trino_bootstrap"]
        R2["BLOCK trino_bootstrap"]
        R3["BLOCK trino_bootstrap"]
        R4["BLOCK + alert"]
        R5["BLOCK superset_bootstrap"]
        R6["Slack warning only"]
        R7["BLOCK analytics_check"]
        R8["Slack warning only"]
        R9["Slack warning only"]
    end

    F1 --> D1 --> R1
    F2 --> D2 --> R2
    F3 --> D3 --> R3
    F4 --> D4 --> R4
    F5 --> D5 --> R5
    F6 --> D6 --> R6
    F7 --> D7 --> R7
    F8 --> D8 --> R8
    F9 --> D9 --> R9

    style F1 fill:#c00,stroke:#333,color:#fff
    style F2 fill:#c00,stroke:#333,color:#fff
    style F3 fill:#c00,stroke:#333,color:#fff
    style F4 fill:#c94,stroke:#333,color:#fff
    style F5 fill:#c00,stroke:#333,color:#fff
    style F6 fill:#e94,stroke:#333,color:#fff
    style F7 fill:#c00,stroke:#333,color:#fff
    style F8 fill:#e94,stroke:#333,color:#fff
    style F9 fill:#e94,stroke:#333,color:#fff
```

---

## 6. Per-Node Output Contract (Example)

```yaml
# contracts/silver.yaml — what spark_batch must produce
node: spark_batch + spark_streaming
output: hive.nyc.trips
owner: data-engineering
checks:
  - name: min_row_count
    query: SELECT COUNT(*) FROM hive.nyc.trips
    assert: "> 1000000"
    severity: CRITICAL
  - name: no_null_fare
    query: SELECT COUNT(*) FROM hive.nyc.trips WHERE fare_amount IS NULL
    assert: "== 0"
    severity: CRITICAL
  - name: distance_sane
    query: SELECT AVG(trip_distance) FROM hive.nyc.trips
    assert: "> 1 AND < 20"
    severity: WARNING
  - name: passenger_count_range
    query: >
      SELECT COUNT(*) FROM hive.nyc.trips
      WHERE passenger_count < 1 OR passenger_count > 6
    assert: "== 0"
    severity: CRITICAL
  - name: location_exists
    query: >
      SELECT COUNT(*) FROM hive.nyc.trips t
      LEFT JOIN hive.nyc.taxi_zone_lookup z
        ON t.pickup_location_id = z.location_id
      WHERE z.location_id IS NULL
    assert: "== 0"
    severity: WARNING
  - name: freshness
    query: SELECT MAX(pickup_date) FROM hive.nyc.trips
    assert: ">= CURRENT_DATE - INTERVAL '60' DAY"
    severity: CRITICAL
  - name: quarantine_ratio
    query: |
      SELECT CAST(q.cnt AS DOUBLE) / NULLIF(s.cnt, 0) * 100.0
      FROM (SELECT COUNT(*) AS cnt FROM hive.nyc.trips) s
      CROSS JOIN (SELECT COUNT(*) AS cnt FROM hive.nyc.invalid_trips) q
    assert: "<= 15"
    severity: WARNING
```

---

## 8. Postgres CDC — Production Hardening

### Hiện trạng

```yaml
# charts/nyc-taxi/templates/postgres-cdc/statefulset.yaml
image: postgres:16-alpine
replicas: 1
args: [wal_level=logical, max_replication_slots=4, max_wal_senders=4]
resources: {cpu: 200m-500m, memory: 512Mi-1Gi}
```

### Vấn đề

| # | Vấn đề | Hậu quả |
|---|---|---|
| 1 | **Không giới hạn WAL size** — không set `max_wal_size` | Debezium chết vài giờ → WAL tích lũy vô hạn → disk full → Postgres crash |
| 2 | **Không `wal_keep_size`** — WAL có thể bị xóa trước khi Debezium kịp đọc | Debezium lag > WAL retention → "requested WAL segment has already been removed" → phải re-snapshot |
| 3 | **Single replica** — không HA | Pod chết → CDC pipeline ngừng → WAL tích lũy |
| 4 | **PVC không backup, không snapshot** | Node die → mất toàn bộ data CDC |
| 5 | **Không connection pooling** | Debezium + cdc_seed + app query → exhaustion |
| 6 | **Không monitor replication slot** | Slot đầy không ai biết → Debezium không start được |
| 7 | **Password plaintext** | `POSTGRES_PASSWORD=postgres` |

### Thiết kế production

```yaml
# Production Postgres CDC config
postgresql:
  # ── WAL management (cân bằng giữa an toàn và ổ cứng) ──
  max_wal_size: 4GB          # WAL tối đa — chặn disk full
  min_wal_size: 1GB          # WAL tối thiểu — giữ cho Debezium lag
  wal_keep_size: 2GB         # Giữ WAL ít nhất 2GB để Debezium catch-up
  max_replication_slots: 5   # Dự phòng: 1 active + 1 spare
  max_wal_senders: 5
  wal_sender_timeout: 60s    # Kill sender nếu Debezium không phản hồi

  # ── Resource (cho 100K rows CDC) ──
  resources:
    requests: {cpu: 500m, memory: 1Gi}
    limits: {cpu: 2, memory: 4Gi}
  shared_buffers: 512MB
  effective_cache_size: 2GB

  # ── Connection pool ──
  max_connections: 100
  # Dùng PgBouncer sidecar nếu nhiều service connect

  # ── Backup (RDS hoặc cron job) ──
  # Option A: pg_dump cron mỗi ngày → S3
  # Option B: WAL archiving → S3 (pitr recovery)
  archive_mode: on
  archive_command: 'aws s3 cp %p s3://nyc-backup/wal/%f'

  # ── Replication slot monitor ──
  # Query: SELECT slot_name, active, restart_lsn, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
  # FROM pg_replication_slots;
  # Alert nếu active=false hoặc lag > 1GB

  # ── High Availability ──
  # Option A: RDS Multi-AZ (managed, auto failover)
  # Option B: Patroni + etcd (self-managed, 3 replicas)
  # Option C: CloudNativePG operator (K8s native)
```

### Strategy: WAL không được đầy, không được mất

```mermaid
flowchart TD
    WAL_SIZE{"WAL size hiện tại?"}
    WAL_SIZE -->|"< 2GB"| OK["OK — bình thường"]
    WAL_SIZE -->|"2-3.5GB"| WARN["WARNING — tăng tần suất Debezium poll<br/>giảm batch size CDC"]
    WAL_SIZE -->|"> 3.5GB"| CRIT["CRITICAL — restart Debezium<br/>nếu không được → tăng max_wal_size tạm<br/>→ alert Slack + Email"]
    WAL_SIZE -->|"> 4GB"| PANIC["PANIC — Postgres từ chối transaction<br/>→ toàn bộ app chết<br/>→ page on-call ngay"]

    style OK fill:#4a4,stroke:#333,color:#fff
    style WARN fill:#e94,stroke:#333,color:#fff
    style CRIT fill:#c00,stroke:#333,color:#fff
    style PANIC fill:#800,stroke:#333,color:#fff
```

### Monitor queries (Monitor DAG @every 5min)

```sql
-- 1. WAL size
SELECT pg_size_pretty(pg_current_wal_lsn() - '0/0') AS wal_size;

-- 2. Replication slot health
SELECT slot_name, active, 
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots;

-- 3. Replication slot count warning
SELECT COUNT(*) FROM pg_replication_slots;
-- Alert nếu = max_replication_slots → sắp đầy

-- 4. Dead tuples (cần VACUUM)
SELECT relname, n_dead_tup FROM pg_stat_user_tables WHERE n_dead_tup > 10000;
```

### Decision: Tự host hay RDS?

| Yếu tố | Self-host (StatefulSet) | AWS RDS |
|---|---|---|
| **WAL management** | Tự config | Auto — `max_allocated_storage` |
| **Backup** | Tự pg_dump + S3 | Auto snapshot + pitr 35 ngày |
| **HA** | Patroni phức tạp | Multi-AZ checkbox |
| **Replication slot** | Tự monitor | CloudWatch metric |
| **Cost** | EC2 + disk | RDS instance cost |
| **Phù hợp cho** | Dev/staging | Production |

**Khuyến nghị:** Dev giữ StatefulSet. Production dùng RDS + `rds.logical_replication=1`.

**Pod count:** Dev = 1 pod (StatefulSet). Production = 0 pod (RDS managed).

---

## 9. Debezium — Production Hardening

### Hiện trạng

```yaml
# DAG: cdc_register → gọi entrypoint-cdc-register → Debezium REST API
# POST /connectors — tạo connector với config:
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "svc-postgres-cdc",
  "slot.name": "nyc_cdc",
  "publication.name": "nyc_cdc_pub"
}
```

### Vấn đề

| # | Vấn đề | Hậu quả |
|---|---|---|
| 1 | **Snapshot initial lần đầu quét toàn bộ table** — chưa set `snapshot.mode` | Debezium lock table + produce vài GB events Kafka → Kafka disk full + spark_streaming overload |
| 2 | **Single-thread per connector** — không scale được theo table | 1 connector = 1 thread WAL + 1 thread produce → nếu nhiều table thay đổi → backlog |
| 3 | **Transform quá nặng** — mặc định serialize cả `before`/`after`/schema → mỗi event vài KB | 1M events/giờ = vài GB Kafka → tốn disk + network |
| 4 | **Kafka producer chưa tune** — không batch, không compress | 1 request/event → network overhead cao → produce chậm |
| 5 | **Offset lưu trong Kafka** — nếu Kafka topic offset bị expire | Debezium mất offset → nghĩ chưa đọc gì → re-snapshot → duplicate toàn bộ |
| 6 | **Không monitor lag** — `MilliSecondsBehindSource` | Lag tăng dần → WAL đầy → không ai biết đến khi DB crash |
| 7 | **Delete event không xử lý** — `after=null` bị bỏ qua | Row xóa trong Postgres → vẫn còn trong silver → data stale |

### Thiết kế production

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  
  "snapshot.mode": "schema_only",
  
  "transforms": "unwrap,route",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "false",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  
  "producer.override.batch.size": "65536",
  "producer.override.linger.ms": "50",
  "producer.override.compression.type": "lz4",
  "producer.override.max.request.size": "1048576",
  
  "max.batch.size": "4096",
  "max.queue.size": "65536",
  "poll.interval.ms": "100",
  
  "offset.storage.topic": "nyc_cdc_offset",
  "offset.storage.partitions": 3,
  "offset.storage.replication.factor": 1,
  "offset.flush.interval.ms": "10000",
  
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.deadletterqueue.topic.name": "nyc_cdc_dlq",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

### Giải thích từng config

| Config | Giá trị | Tại sao |
|---|---|---|
| `snapshot.mode=schema_only` | Chỉ lấy schema, không snapshot data | Data đã có từ batch, không cần duplicate. Tránh lock table + full scan |
| `transforms=ExtractNewRecordState` | Chỉ lấy `after`, bỏ `before` + schema metadata | Giảm event size 70% → Kafka disk tiết kiệm |
| `delete.handling.mode=rewrite` | Delete event → produce event với `__deleted=true` | Không mất delete, consumer tự lọc |
| `batch.size=65536 + linger.ms=50` | Gộp event thành batch 64KB, đợi 50ms | Giảm network request 10-50x |
| `max.batch.size=4096` | Đọc tối đa 4096 event/lần từ WAL | Tránh 1 lần đọc quá nhiều → OOM |
| `offset.storage.topic=nyc_cdc_offset` | Lưu offset trong Kafka topic riêng | Nếu topic chính expire, offset vẫn còn |
| `errors.deadletterqueue.topic` | Event lỗi → DLQ topic | Không drop event, replay được |

### Monitor Debezium (Monitor DAG)

```
GET /connectors/nyc-cdc-connector/status

Check:
  connector.state == "RUNNING"           → PASS
  tasks[0].state == "RUNNING"            → PASS
  MilliSecondsBehindSource < 300000       → PASS (< 5 phút)
  MilliSecondsBehindSource > 300000       → WARNING → Slack
  connector.state == "FAILED"            → CRITICAL → Slack + Email
```

### Decision: Giữ Debezium hay bỏ?

| Yếu tố | Giữ Debezium | Bỏ Debezium, dùng polling |
|---|---|---|
| **Real-time** | ✅ Dưới 1 giây | ❌ Delay 5-60 phút |
| **Độ phức tạp** | ❌ Kafka + Connect cluster | ✅ Chỉ cần Python script + cron |
| **Bắt delete** | ✅ Auto | ❌ Phải soft-delete + flag |
| **DB load** | ✅ Đọc WAL, nhẹ | ❌ SELECT query liên tục |
| **Phù hợp** | Production multi-table CDC | Demo/single-table/không cần real-time |

**Khuyến nghị:** Giữ Debezium nếu có >1 table cần CDC + cần real-time. Pipeline này monthly batch → polling đủ dùng, bỏ Debezium cho đỡ phức tạp.

**Pod count:** Dev = 1 pod. Production = 1 pod (K8s auto-restart là đủ).

---

## 10. Kafka — Production Hardening

### Hiện trạng

```yaml
# charts/nyc-taxi/templates/kafka/statefulset.yaml
image: confluentinc/cp-kafka:7.4.0
replicas: 1
resources: {cpu: 500m-1, memory: 1Gi-2Gi}
topics: [taxi.trip.events, nyc_cdc.public.trips]
```

### Vấn đề

| # | Vấn đề | Hậu quả |
|---|---|---|
| 1 | **Single broker + 1 partition/topic** — không scale | 1 consumer max → spark_streaming single-thread |
| 2 | **Retention = 7 ngày mặc định** | Spark streaming nghỉ >7 ngày → offset cũ bị xóa → re-read từ earliest → duplicate hoặc failOnDataLoss crash |
| 3 | **Producer không idempotent** — cdc_bridge retry → duplicate message | Network fail → retry → 1 row CDC thành 2 message trong Kafka → spark_streaming duplicate silver |
| 4 | **No compaction** — delete tombstone tích lũy nhưng không merge | Topic chỉ thêm mới, không bao giờ giảm → disk tăng vô hạn |
| 5 | **No DLQ topic** — message hỏng drop âm thầm | spark_streaming crash với poison pill → recover bằng cách skip offset → mất message |
| 6 | **No consumer group monitoring** | Không biết spark_streaming đang lag bao nhiêu offset |

### Thiết kế production

```properties
# ── Topic config ──
taxi.trip.events:
  partitions: 3
  retention.ms: 1209600000       # 14 ngày
  cleanup.policy: compact,delete # compact tombstone + delete hết hạn
  min.cleanable.dirty.ratio: 0.5

taxi.trip.events.dlq:
  partitions: 1
  retention.ms: 2592000000       # 30 ngày — giữ lâu để debug

nyc_cdc_offset:
  partitions: 3
  retention.ms: -1               # Không expire — offset không được mất
  cleanup.policy: compact

# ── Broker config ──
num.partitions: 3
log.retention.hours: 336         # 14 ngày
log.segment.bytes: 268435456     # 256MB segment
auto.create.topics.enable: false # Cấm auto-create topic

# ── Producer config (cdc_bridge) ──
enable.idempotence: true         # Chống duplicate
acks: all                        # Chờ tất cả ISR confirm
compression.type: lz4
linger.ms: 50
batch.size: 65536

# ── Consumer config (spark_streaming) ──
group.id: spark-stream-nyc-v1    # Cố định group → track offset
auto.offset.reset: latest        # Nếu mất offset → đọc từ latest (không re-process 14 ngày)
enable.auto.commit: false        # Spark tự quản lý offset qua checkpoint
isolation.level: read_committed  # Chỉ đọc transaction đã commit
```

### Giải thích từng config

| Config | Giá trị | Tại sao |
|---|---|---|
| `partitions: 3` | 3 partition/topic | Spark streaming có thể parallel 3 consumer → nhanh 3x. Đủ cho vài triệu event/ngày |
| `retention: 14 ngày` | Đủ dài để spark recover | Nếu spark chết 1 tuần → vẫn đọc được backlog |
| `cleanup.policy=compact,delete` | Compaction merge key trùng + xóa hết hạn | Tombstone delete được merge → disk không tăng vô hạn |
| `enable.idempotence=true` | Producer không duplicate | cdc_bridge retry → Kafka biết message đã tồn tại → dedup |
| `auto.create.topics.enable=false` | Cấm auto-create | Tránh tạo topic rác khi gõ sai tên |
| `group.id cố định` | group cố định, không random | Spark track offset qua lần restart → không duplicate |
| `auto.offset.reset=latest` | Mất offset → đọc từ latest | Thà bỏ qua backlog còn hơn duplicate toàn bộ 14 ngày data |

### Monitor Kafka (Monitor DAG)

```
kafka-consumer-groups --bootstrap-server svc-kafka:9092 \
  --group spark-stream-nyc-v1 --describe

Check:
  LAG per partition < 1000      → PASS
  LAG per partition > 1000      → WARNING → Slack
  LAG per partition > 10000     → CRITICAL → Slack + Email
  DLQ topic message count > 0   → WARNING → Slack (có poison pill cần xem)
  Broker disk usage < 85%       → PASS
  Broker disk usage > 85%       → CRITICAL → Slack + Email
```

**Pod count:** Dev = 1 pod (1 broker, đủ cho demo). Production = 3 pod (3 broker cluster — chịu được 1 broker chết không mất data).

### CDC Chain Pod Summary

| Component | Dev | Production | Ghi chú |
|---|---|---|---|
| Postgres CDC | 1 pod (StatefulSet) | 0 pod (RDS) | RDS lo HA + backup |
| Debezium | 1 pod | 1 pod | K8s auto-restart |
| Kafka | 1 pod (1 broker) | 3 pod (3 broker) | Production cần cluster |
| **Tổng** | **3 pod** | **4 pod + RDS** | |

---

## 11. Implementation Priority

```mermaid
gantt
    title Quality Gate Implementation Roadmap
    dateFormat  YYYY-MM-DD
    axisFormat  %m/%d

    section Phase 1 — Critical Gates
    verify_silver (rows, null, dist)     :p1, 2026-07-01, 3d
    verify_freshness (staleness)         :p1b, after p1, 1d

    section Phase 2 — Export Gates
    verify_postgres (pg vs gold)         :p2, after p1b, 2d
    verify_gold (30 tables complete)     :p2b, after p2, 1d

    section Phase 3 — Presentation
    verify_superset (charts render)      :p3, after p2b, 2d
    Cross-node reconciliation (4 pairs) :p3b, after p3, 2d

    section Phase 4 — Monitor DAG
    Monitor DAG (7 health checks @hourly):p4, after p3b, 3d
    Alert pipeline (Slack + Email)       :p4b, after p4, 2d

    section Phase 5 — Contracts
    Output contracts YAML                :p5, after p4b, 3d
    Pipeline Health Dashboard (Superset) :p5b, after p5, 2d
```

---

## Summary

| Component | Purpose | Runs | Blocks Pipeline? |
|---|---|---|---|
| **MAIN DAG** | Pipeline chính | Monthly | — |
| **Quality Gates** | Verify output từng node | Inline trong MAIN | ✅ Yes |
| **Monitor DAG** | Giám sát liên tục ngoài pipeline | @hourly, song song | ❌ No (alert only) |
| **Output Contracts** | Định nghĩa "data tốt" mỗi node | Manual review | N/A |
| **Health Dashboard** | Superset dashboard 🟢🟠🔴 | Refresh 5 phút | N/A |
| **Reconciliation** | Row count cross-check giữa các tầng | Inline trong MAIN | ✅ Yes |
