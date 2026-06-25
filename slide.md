# NYC Taxi Data Pipeline

> Slide deck — trình bày cho management

---

## Slide 1: Title

**NYC Taxi Data Pipeline**  
*Batch & Streaming Data Engineering Platform*

```mermaid
%%{init: {'theme': 'default', 'themeVariables': {'background': '#ffffff'}}}%%
flowchart LR
    TITLE["🚕 NYC TAXI DATA PIPELINE"]
    SUB["Batch · Streaming · CDC · 8.4M trips · 2 outputs"]

    DESC["Xử lý dữ liệu NYC TLC<br/>từ raw Parquet và Postgres CDC<br/>→ Golden Data cho Data Engineer<br/>→ Dashboard cho Marketing / Sales / CEO"]

    STACK["Spark · MinIO · Trino · dbt · Airflow · Superset<br/>Postgres · Debezium · Kafka · Kubernetes"]

    TITLE --> SUB --> DESC --> STACK

    style TITLE fill:#fff,stroke:#e94,stroke-width:3px,color:#1a1a2e
    style SUB fill:#f5f5f5,stroke:#0f3460,stroke-width:2px,color:#16213e
    style DESC fill:#fff,stroke:#48a,stroke-width:2px,color:#1a2a3e
    style STACK fill:#f5f5f5,stroke:#555,stroke-width:1px,color:#333
```

---

## Slide 2: Bài toán

### Input

| Nguồn | Format | Volume |
|---|---|---|
| 📦 NYC TLC Yellow Taxi (S3 Parquet) | Hive-partitioned | 8.4M trips / 3 tháng |
| 📨 Postgres CDC (Debezium → Kafka) | Logical replication | Real-time |

### Output — 2 sản phẩm, 2 đối tượng

| Sản phẩm | Format | Cho ai | Họ làm gì |
|---|---|---|---|
| **📦 Golden Data** | 30 Parquet datasets | Data Engineers | Pipeline mới, train model, audit |
| **📊 Superset Dashboard** | PostgreSQL → 4 charts | Marketing, Sales, CEO | Quyết định kinh doanh |

---

## Slide 3: Hai luồng song song

```mermaid
%%{init: {'theme': 'default', 'themeVariables': {'background': '#ffffff'}}}%%
flowchart LR
    subgraph MAIN["MAIN FLOW — pipeline chính (chạy monthly)"]
        direction LR
        M1["spark_batch"] --> M2["trino_bootstrap"]
        M0["spark_streaming"] --> M2
        M2 --> M3["dbt_build"]
        M3 --> M4["gold_export"]
        M3 --> M5["materialize → Postgres"]
        M5 --> M6["superset"]
        M6 --> M7["analytics_check"]
    end

    subgraph MONITOR["MONITOR FLOW — giám sát song song (@every 5min)"]
        direction LR
        N1["check_silver"]
        N0["check_streaming"]
        N2["check_gold"]
        N3["check_postgres"]
        N4["check_superset"]
        N5["check_freshness"]
        N_cdc1["check_pg_cdc"]
        N_cdc2["check_debezium"]
        N_cdc3["check_kafka"]
    end

    M1 -.->|"quan sát"| N1
    M0 -.->|"quan sát"| N0
    M0 -.->|"CDC chain"| N_cdc1 & N_cdc2 & N_cdc3
    M4 -.->|"quan sát"| N2
    M5 -.->|"quan sát"| N3
    M6 -.->|"quan sát"| N4
    M7 -.->|"quan sát"| N5

    N0 & N1 & N2 & N3 & N4 & N5 & N_cdc1 & N_cdc2 & N_cdc3 --> ALERT["🚨 Slack + Email"]

    style MAIN fill:#fff,stroke:#48a,stroke-width:2px,color:#1a2a3e
    style MONITOR fill:#fff5f5,stroke:#c00,stroke-width:2px,color:#600
    style ALERT fill:#fff0f0,stroke:#c00,stroke-width:3px,color:#c00
```

| | MAIN FLOW | MONITOR FLOW |
|---|---|---|
| **Vai trò** | Chạy pipeline, không thay đổi gì | DAG riêng, chỉ SELECT, không ghi |
| **Tần suất** | Monthly (batch) | @every 5 phút |
| **Nếu fail** | Pipeline dừng, phải retry | Slack + Email ngay |
| **Phụ thuộc** | Không phụ thuộc Monitor | Không ảnh hưởng MAIN |

---

## Slide 4: Pipeline — 13 nodes

```mermaid
%%{init: {'theme': 'default', 'themeVariables': {'background': '#ffffff'}}}%%
flowchart TD
    subgraph SOURCE["SOURCE"]
        RAW["📦 nyc-raw Parquet"]
        PG["🐘 Postgres CDC"]
    end

    subgraph INGEST["INGEST — Spark"]
        SB["⚡ spark_batch"]
        SS["⚡ spark_streaming"]
    end

    subgraph STORAGE["MinIO S3"]
        SILVER["✅ nyc-silver · 265MB"]
        QUAR["⚠️ nyc-quarantine · 36MB"]
        LOOKUP["📋 nyc-lookup · 12KB"]
        GOLD["📦 nyc-gold · 30 datasets"]
    end

    subgraph CATALOG["CATALOG — Trino"]
        TRINO["Trino 435 · hive.nyc.*"]
    end

    subgraph TRANSFORM["TRANSFORM — dbt"]
        DBT["dbt 30 models · 54 tests"]
    end

    subgraph EXPORT["EXPORT"]
        GE["gold_export → MinIO"]
        MP["materialize → Postgres"]
    end

    subgraph ANALYTICS["ANALYTICS"]
        PGA["Postgres nyc_analytics"]
        SUP["Superset · 7 datasets"]
    end

    subgraph CDC["CDC PATH"]
        DZ["Debezium 2.5"]
        KFK["Kafka"]
        BRIDGE["cdc_bridge"]
    end

    RAW --> SB
    PG --> DZ --> KFK --> BRIDGE --> SS
    SB --> SILVER & QUAR
    SS --> SILVER & QUAR
    LOOKUP --> SB & SS
    SILVER & QUAR & LOOKUP --> TRINO
    TRINO --> DBT
    DBT --> GE & MP
    GE --> GOLD
    MP --> PGA --> SUP

    style SOURCE fill:#fff,stroke:#48a,stroke-width:2px,color:#1a2a3e
    style INGEST fill:#fff,stroke:#4a4,stroke-width:2px,color:#1a3a1a
    style STORAGE fill:#fff,stroke:#4a9,stroke-width:2px,color:#1a2a2e
    style CATALOG fill:#fff,stroke:#94a,stroke-width:2px,color:#2a1a2e
    style TRANSFORM fill:#fff,stroke:#ca4,stroke-width:2px,color:#2a2a1e
    style EXPORT fill:#fff,stroke:#e94,stroke-width:2px,color:#2d132c
    style ANALYTICS fill:#fff,stroke:#a4a,stroke-width:2px,color:#2a1a2e
    style CDC fill:#fff,stroke:#689,stroke-width:2px,color:#1a2a3e
```

---

## Slide 5: Quality Gates — 5 gates, block pipeline

```mermaid
%%{init: {'theme': 'default', 'themeVariables': {'background': '#ffffff'}}}%%
flowchart LR
    subgraph GATES["QUALITY GATES — kiểm tra output mỗi node"]
        V1["verify_silver<br/>✓ row_count > 0<br/>✓ null_ratio = 0<br/>✓ distance in range<br/>✓ MAX(date) fresh"]
        V2["verify_gold<br/>✓ 30 tables exist<br/>✓ each row_count > 0"]
        V3["verify_postgres<br/>✓ pg rows = gold rows"]
        V4["verify_superset<br/>✓ charts render OK<br/>✓ metrics match Trino"]
        V5["verify_freshness<br/>✓ MAX(date) ≤ 35d<br/>✓ rows in 7d range"]
    end

    BATCH["spark_batch"] --> V1
    STREAM["spark_streaming"] --> V1
    V1 -->|PASS| TB["trino → dbt → ..."]
    V1 -->|FAIL| BLOCK["⛔ BLOCK + Slack + Email"]

    DBT["dbt_build"] --> GE["gold_export"] --> V2 -->|FAIL| BLOCK
    DBT --> MP["materialize"] --> V3 -->|PASS| SUP["superset"]
    V3 -->|FAIL| BLOCK

    SUP --> V5["verify_superset"] -->|PASS| AC["analytics_check"]
    V5 -->|FAIL| SLACK["Slack warning"]

    style V1 fill:#fff,stroke:#c00,stroke-width:2px,color:#600
    style V2 fill:#fff,stroke:#c00,stroke-width:2px,color:#600
    style V3 fill:#fff,stroke:#c00,stroke-width:2px,color:#600
    style V4 fill:#fff,stroke:#e94,stroke-width:2px,color:#600
    style V5 fill:#fff,stroke:#c00,stroke-width:2px,color:#600
    style BLOCK fill:#fff0f0,stroke:#c00,stroke-width:3px,color:#c00
```

### Cross-Node Reconciliation — đối chiếu xuyên tầng

```
spark_input rows  ==  silver rows + quarantine rows
silver rows       ==  mart.fact_trips rows
mart.* rows       ==  gold_export.* rows
gold.* rows       ==  postgres.* rows
```

Mỗi cặp sai → block pipeline ngay.

---

## Slide 6: Failure Mode → Detection → Ứng phó

| # | Tình huống | Phát hiện bởi | Ứng phó |
|---|---|---|---|
| 1 | MinIO sai bucket → Spark đọc 0 rows | verify_silver: row_count = 0 | **Block** trino_bootstrap |
| 2 | Spark type cast → fare_amount null | verify_silver: null_ratio > 0 | **Block** trino_bootstrap |
| 3 | Spark crash → duplicate rows | verify_silver: row_count > expected | **Block** trino_bootstrap |
| 4 | Trino OOM → gold export thiếu bảng | verify_gold: tables < 30 | **Block** + alert |
| 5 | materialize fail → Postgres empty | verify_postgres: pg ≠ gold | **Block** superset |
| 6 | Superset cache → data cũ | verify_superset: chart stale | Slack warning |
| 7 | NYC không publish data mới | verify_freshness: max_date > 35d | **Block** analytics_check |
| 8 | Kafka consumer lag | check_kafka: lag > 1000 | Slack + Email |
| 9 | MinIO disk gần đầy | check_minio: size > 2x avg | Slack warning |

---

## Slide 7: CDC Chain — Postgres → Debezium → Kafka → Streaming

### Postgres CDC

| Vấn đề | Fix | Tại sao |
|---|---|---|
| WAL không giới hạn → disk full | `max_wal_size=4GB`, `wal_keep_size=2GB` | Chặn DB crash |
| Replication slot mất → mất offset | Monitor slot active + lag | Phát hiện < 5 phút |
| Single point of failure | Dev: StatefulSet + backup. Production: RDS Multi-AZ | HA |

### Debezium

| Vấn đề | Fix | Tại sao |
|---|---|---|
| Snapshot full table → lock DB | `snapshot.mode=schema_only` | Data đã có từ batch |
| Event lớn (before + after + schema) | `ExtractNewRecordState` | Giảm 70% event size |
| Delete event mất | `delete.handling.mode=rewrite` | Có tombstone |
| Không monitor lag | Check `MilliSecondsBehindSource` < 5 phút | Alert kịp thời |

### Kafka

| Vấn đề | Fix | Tại sao |
|---|---|---|
| 1 partition → 1 consumer | 3 partitions | Spark parallel 3x |
| 7d retention → mất offset | 14 ngày + offset topic retention = -1 | Recover được sau 1 tuần |
| Producer duplicate | `enable.idempotence=true`, `acks=all` | Retry không duplicate |

### Spark Streaming

| Vấn đề | Fix | Tại sao |
|---|---|---|
| Ghi chung path với batch | Tách `nyc-silver/stream/trips` | Batch + stream độc lập |
| group_id random → duplicate | `group.id` cố định | Track offset qua restart |
| Poison pill → crash | DLQ topic + try-catch | Stream không chết |
| `trigger=availableNow` → không real-time | `processingTime=5min` | Continuous |
| Delete event mất | Soft delete `is_deleted=true` | dbt filter `WHERE is_deleted=false` |

---

## Slide 8: MinIO — Intake Validation

**Vấn đề:** 1 file Parquet sai schema → Spark crash cả batch. File sai tên → glob miss → thiếu data.

**Giải pháp:** `validate_raw_files.py` chạy trước spark_batch.

```mermaid
%%{init: {'theme': 'default', 'themeVariables': {'background': '#ffffff'}}}%%
flowchart LR
    RAW["s3://nyc-raw/"] --> VAL["validate_raw_files.py"]
    VAL -->|PASS| SPARK["spark_batch"]
    VAL -->|FAIL| QUAR["_quarantine/ + Slack"]
    VAL -->|DUPLICATE| SKIP["Bỏ qua"]
    SPARK --> SILVER["nyc-silver"]

    style RAW fill:#fff,stroke:#48a,stroke-width:2px,color:#1a2a3e
    style VAL fill:#fff,stroke:#e94,stroke-width:2px,color:#600
    style QUAR fill:#fff0f0,stroke:#c00,stroke-width:2px,color:#c00
    style SPARK fill:#fff,stroke:#4a4,stroke-width:2px,color:#1a3a1a
```

**3 levels:**
1. Path & format — `year=*/month=*/*.parquet`
2. Schema — đủ 19 cột Parquet bắt buộc
3. Duplicate — check etag, không xử lý lại file cũ

---

## Slide 9: Trino — Chống OOM, tăng concurrency

| Vấn đề | Fix |
|---|---|
| OOM khi gold_export 30 CTAS | Resource group: gold_export max 2 concurrent, 3GB/query |
| max 1 concurrent query | Tăng lên 5 concurrent |
| File-based metastore corrupt | Dev: backup PVC → S3. Production: Glue Catalog |
| Partition không tự sync | Auto sync sau spark_batch |
| Single node – không HA | Production: 1 coordinator + 2 workers |
| Không query monitoring | Monitor DAG check `system.runtime.queries` |

---

## Slide 10: dbt + Superset + Anomaly Check

### dbt

| Vấn đề | Fix |
|---|---|
| Không CI/CD — push thẳng | GitHub Actions: `dbt build --target staging` trên PR |
| Chỉ test not_null | Business assertion: SUM(revenue) cross-check |
| Scan toàn bộ 180M rows mỗi lần | Incremental model cho fact_trips |
| Không data lineage | `dbt docs generate` → host S3 |

### Superset

| Vấn đề | Fix |
|---|---|
| Bootstrap tạo duplicate | Check tồn tại trước khi tạo |
| Cache stale sau pipeline | API refresh dataset + chart sau materialize |
| Chart SQL không version | Lưu trong `superset/charts/`, PR review |
| admin/admin public | Env var + public user read-only |

### Anomaly Check

| Vấn đề | Fix |
|---|---|
| exit code luôn 0 (info only) | Thêm flag `--block` để block pipeline |
| Chỉ check row count | Thêm fare, distance, revenue |
| Không alert | Slack webhook khi phát hiện |
| Threshold cứng | 30-day rolling baseline ± 3σ |

---

## Slide 11: Lộ trình — 5 phase

```mermaid
%%{init: {'theme': 'default', 'themeVariables': {'background': '#ffffff'}}}%%
gantt
    title Production Hardening Roadmap
    dateFormat  YYYY-MM-DD
    axisFormat  Tuần %W

    section Phase 1
    Intake validation + Quality gates     :p1, 2026-07-01, 2w

    section Phase 2
    Monitor DAG + Alert                   :p2, after p1, 2w

    section Phase 3
    CDC Chain (Postgres · Kafka · Stream) :p3, after p2, 2w

    section Phase 4
    Trino + dbt + Superset                :p4, after p3, 2w

    section Phase 5
    Anomaly + Health Dashboard + Contracts:p5, after p4, 2w
```

### Tổng kết

| Mục | Chi tiết |
|---|---|
| **Hiện tại** | Pipeline chạy ổn định, 8.4M trips, 3 DAGs, 12 pods |
| **Hardening** | 5 phase, 10 tuần, 1-2 người |
| **Mục tiêu** | Tự phát hiện + ứng phó sự cố trong < 5 phút |
| **Chi phí infra** | +3-5 pod (Kafka cluster, Trino workers) |
