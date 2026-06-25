# NYC Taxi Pipeline — Production Design

> Từ PoC lên production-ready. Mỗi chương: Vấn đề → Giải pháp → Config tham khảo.

---

## Chương 1: Tổng quan — Pipeline hiện tại vs Tương lai

### Hiện tại: 1 luồng, không monitor, không gate

```mermaid
flowchart LR
    BATCH["spark_batch"] --> TRINO["trino_bootstrap<br/>(one_success)"]
    STREAM["spark_streaming"] --> TRINO
    TRINO --> DBT["dbt_build"]
    DBT --> GE["gold_export"]
    DBT --> MP["materialize"]
    MP --> SUP["superset"]
    SUP --> AC["analytics_check<br/>(info only)"]
```

**Vấn đề:** 11/13 node không ai check output. Data sai ở bất kỳ node nào → lan ra tới Superset mới phát hiện.

### Tương lai: MAIN pipeline + MONITOR song song

```mermaid
flowchart LR
    subgraph MAIN["MAIN FLOW — chạy monthly, không thay đổi"]
        M1["spark_batch"] --> M2["trino_bootstrap"]
        M0["spark_streaming"] --> M2
        M2 --> M3["dbt_build"]
        M3 --> M4["gold_export"]
        M3 --> M5["materialize → Postgres"]
        M5 --> M6["superset"]
    end

    subgraph MONITOR["MONITOR FLOW — @every 5min, read-only"]
        N1["check_silver"]
        N2["check_gold"]
        N3["check_postgres"]
        N4["check_superset"]
        N5["check_freshness"]
        N6["check_pg_cdc / debezium / kafka"]
    end

    M1 -.-> N1
    M0 -.-> N6
    M3 -.-> N2
    M5 -.-> N3
    M6 -.-> N4
    M6 -.-> N5
    N1 & N2 & N3 & N4 & N5 & N6 --> ALERT["🚨 Slack + Email"]

    style MAIN fill:#1a1a2e,stroke:#555,color:#ddd
    style MONITOR fill:#16213e,stroke:#0f3460,color:#ddd
    style ALERT fill:#c00,stroke:#333,color:#fff
```

**Khác biệt:**
- MAIN chạy như cũ. MONITOR chạy song song, độc lập.
- Mỗi node có 1 check tương ứng. Fail → Slack + Email, không cần đợi pipeline chạy.
- Monitor fail **không** ảnh hưởng MAIN.

### Pipeline Nodes — toàn cảnh

```mermaid
flowchart TD
    RAW["MinIO nyc-raw<br/>Parquet"] --> SB["spark_batch"]
    PG["Postgres CDC"] --> DZ["debezium"] --> KFK["Kafka"] --> SS["spark_streaming"]
    SB -->|valid| SILVER["nyc-silver"]
    SB -->|invalid| QUAR["nyc-quarantine"]
    SS -->|valid| SILVER
    SS -->|invalid| QUAR
    SILVER --> TRINO["Trino"]
    QUAR --> TRINO
    TRINO --> DBT["dbt 30 models / 54 tests"]
    DBT --> GE["gold_export → nyc-gold"]
    DBT --> MP["materialize → Postgres"]
    MP --> SUP["Superset Dashboard"]
    TRINO --> SUP
```

---

## Chương 2: Monitor & Quality Gates

### Vấn đề: 11/13 node không ai kiểm tra output

Pipeline chạy success, data sai logic → không ai phát hiện cho đến khi người nhìn Superset thấy số vô lý. Lúc đó không biết lỗi từ node nào.

### Giải pháp: 2 lớp bảo vệ

**Lớp 1 — Quality Gates (inline, block pipeline):**

```mermaid
flowchart LR
    SB["spark_batch + streaming"] --> V1["verify_silver"]
    V1 -->|PASS| TB["trino_bootstrap → dbt → ..."]
    V1 -->|FAIL| BLOCK["⛔ BLOCK pipeline → Slack + Email"]

    DBT2["dbt_build"] --> GE2["gold_export"] --> V2["verify_gold"]
    DBT2 --> MP2["materialize"] --> V3["verify_postgres"]
    V3 -->|PASS| SUP2["superset"]
    V3 -->|FAIL| BLOCK

    SUP2 --> V4["verify_superset"]
    V4 -->|FAIL| SLACK["Slack warning"]

    V5["verify_freshness"] --> AC["analytics_check"]
    V5 -->|FAIL| BLOCK

    style BLOCK fill:#c00,stroke:#333,color:#fff
    style V1 fill:#e44,stroke:#333,color:#fff
    style V2 fill:#e44,stroke:#333,color:#fff
    style V3 fill:#e44,stroke:#333,color:#fff
    style V4 fill:#e94,stroke:#333,color:#fff
    style V5 fill:#e44,stroke:#333,color:#fff
```

| Gate | Check gì | Query từ đâu | Block ai nếu fail |
|---|---|---|---|
| `verify_silver` | Row count > 0, null ratio = 0, AVG(distance) ∈ [1,20], MAX(date) fresh | Trino `hive.nyc.trips` | `trino_bootstrap` + toàn bộ downstream |
| `verify_gold` | 30/30 tables exist, row count > 0, match dbt source | Trino `hive.nyc_gold.*` | Alert only |
| `verify_postgres` | pg rows = gold rows (từng bảng) | Postgres vs Trino | `superset_bootstrap` |
| `verify_superset` | Charts render OK, metrics match Trino | Superset API + Trino | Slack warning |
| `verify_freshness` | MAX(pickup_date) ≤ 35d, rows trong 7d range | Trino | `analytics_check` |

**Lớp 2 — Monitor DAG (out-of-band, @every 5min):**

```mermaid
flowchart TD
    TRIGGER["⏰ @every 5min"] --> C1["check_silver"]
    TRIGGER --> C2["check_gold"]
    TRIGGER --> C3["check_postgres"]
    TRIGGER --> C4["check_superset"]
    TRIGGER --> C5["check_freshness"]
    TRIGGER --> C6["check_pg_cdc"]
    TRIGGER --> C7["check_debezium"]
    TRIGGER --> C8["check_kafka"]
    
    C1 & C2 & C3 & C4 & C5 & C6 & C7 & C8 --> AGG["Aggregate → PASS/WARN/FAIL"]
    AGG --> DB[("Postgres pipeline_health.checks")]
    DB --> DASH["Superset Health Dashboard<br/>🟢🟠🔴 per node"]
    AGG --> ALERT_M["🚨 FAIL → Slack + Email"]
```

### Reconciliation — cross-node row count

```
spark_input == silver + quarantine
silver       == mart.fact_trips
mart.*       == gold_export.*
gold.*       == postgres.*
```

Mỗi cặp sai → block pipeline. Bắt được data biến mất ở bất kỳ tầng nào.

### Anomaly Check — nâng cấp

| Hiện tại | Mới |
|---|---|
| Chỉ check row count | Check fare_amount, trip_distance, total_revenue |
| Exit code luôn 0 (không block) | Block nếu anomaly > 10% số ngày |
| Log ra stdout | Slack alert + ghi vào Postgres |
| Threshold cứng | 30-day rolling baseline ± 3σ |

---

## Chương 3: CDC Chain — Từ Postgres đến Spark Streaming

### Luồng dữ liệu

```mermaid
flowchart LR
    PG["Postgres CDC<br/>WAL logical"] -->|"đọc WAL"| DZ["Debezium<br/>schema_only snapshot"]
    DZ -->|"produce"| K1["Kafka<br/>nyc_cdc.public.trips"]
    K1 -->|"consume + transform"| BRIDGE["cdc_bridge"]
    BRIDGE -->|"produce"| K2["Kafka<br/>taxi.trip.events<br/>+ DLQ topic"]
    K2 -->|"consume"| SS["spark_streaming<br/>trigger=5min"]
    SS -->|"append"| SILVER["nyc-silver/stream/trips<br/>tách riêng batch"]
    K2 -.->|"poison pill"| DLQ["DLQ topic<br/>retention 30d"]

    style PG fill:#48a,stroke:#333,color:#fff
    style DZ fill:#c94,stroke:#333,color:#fff
    style K2 fill:#694,stroke:#333,color:#fff
    style SS fill:#4a9,stroke:#333,color:#fff
    style DLQ fill:#e44,stroke:#333,color:#fff
```

### Fix cho từng thành phần

#### Postgres CDC

| Vấn đề | Fix |
|---|---|
| WAL đầy ổ → DB crash | Set `max_wal_size=4GB`, `wal_keep_size=2GB` |
| Replication slot mất | Monitor slot active + lag. Backup slot info |
| Single point of failure | Dev: StatefulSet + PVC backup. Production: RDS Multi-AZ |

#### Debezium

| Vấn đề | Fix |
|---|---|
| Snapshot ban đầu quét toàn bộ DB | `snapshot.mode=schema_only` — không cần duplicate data batch |
| Transform quá nặng | `ExtractNewRecordState` — giảm 70% event size |
| Delete event bị bỏ qua | `delete.handling.mode=rewrite` → produce tombstone |
| Không monitor lag | Monitor DAG check `MilliSecondsBehindSource < 5 phút` |

#### Kafka

| Vấn đề | Fix |
|---|---|
| 1 broker → single point | Production: 3 broker cluster |
| 1 partition → single consumer | 3 partitions → spark streaming parallel 3x |
| Producer duplicate | `enable.idempotence=true`, `acks=all` |
| Offset mất → re-process | Offset topic retention = -1 (vô hạn), consumer group cố định |

#### Spark Streaming

| Vấn đề | Fix |
|---|---|
| Ghi chung path với batch → conflict | Tách `nyc-silver/stream/trips` riêng. Merge ở dbt staging `UNION ALL` |
| cdc_bridge duplicate | `group_id` cố định + `enable_auto_commit` |
| Poison pill crash cả stream | Try-catch → DLQ topic → stream tiếp tục |
| `trigger=availableNow` → không real-time | `processingTime="5min"` |
| Delete event bị bỏ qua | Soft delete: `is_deleted=true`, dbt filter `WHERE is_deleted=false` |

### CDC chain bị sập — ai phát hiện?

| Thành phần | Monitor check | Thời gian phát hiện |
|---|---|---|
| Postgres | SELECT 1 + WAL size + slot active | < 5 phút |
| Debezium | GET /connectors/status | < 5 phút |
| Kafka | Consumer lag + broker health | < 5 phút |
| Spark Streaming | Consumer lag + silver stream row count | < 5 phút |

### Pod count

| Component | Dev | Production |
|---|---|---|
| Postgres | 1 pod (StatefulSet) | RDS |
| Debezium | 1 pod | 1 pod |
| Kafka | 1 pod (1 broker) | 3 pod (3 broker) |
| Spark Streaming | 0 (chạy trong Airflow task) | 0 |

---

## Chương 4: Storage & Query — Từ MinIO đến Superset

### MinIO Intake — chặn file hỏng trước khi vào Spark

**Vấn đề:** 1 file Parquet sai schema → Spark crash cả batch. File sai tên → glob miss → data thiếu không ai biết.

**Giải pháp:** `validate_raw_files.py` chạy trước spark_batch.

```mermaid
flowchart LR
    UPLOAD["User upload"] --> VAL["validate_raw_files.py"]
    VAL -->|PASS| SPARK["spark_batch"]
    VAL -->|FAIL| QUAR["_quarantine/ + Slack"]
    VAL -->|DUPLICATE| SKIP["Bỏ qua + log"]
    SPARK --> SILVER["nyc-silver"]
```

**Rule:** Tất cả file PHẢI theo format `year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet`. Không hỗ trợ daily/weekly/yearly/flat. Sai → quarantine.

### Trino — chống OOM, tăng concurrency

**Vấn đề:** gold_export 30 CTAS liên tiếp → Trino OOMKilled. Max 1 query → mọi thứ xếp hàng.

**Giải pháp:**

| Config | Cũ | Mới |
|---|---|---|
| `query.max-memory` | 4GB | 8GB |
| `query.max-concurrent-queries` | 1 | 5 |
| Resource group | Không có | `gold_export`: max 2 query, 3GB/query. `adhoc`: max 3 query, 2GB/query |
| gold_export batch | Chạy 30 CTAS liên tiếp | 3 batch × 10, mỗi batch nghỉ 30s |
| Metastore | File-based (PVC) | Dev: backup PVC. Production: Glue Catalog |
| HA | 1 pod | Dev: 1 pod. Production: 1 coordinator + 2 workers |

```mermaid
flowchart LR
    CO["coordinator"] --> W1["worker-1"] & W2["worker-2"]
    CO --> RG["Resource Groups"]
    RG --> RG1["gold_export: 2 concurrent"]
    RG --> RG2["adhoc: 3 concurrent"]
    W1 & W2 --> S3["MinIO / S3"]
    CO --> GLUE["Glue Catalog"]
```

### dbt — CI/CD, incremental, business test

| Vấn đề | Fix |
|---|---|
| Đổi model không test trước merge | GitHub Actions: `dbt build --target staging` trên PR |
| `fact_trips` scan 180M rows mỗi lần | `materialized='incremental'`, unique_key=`trip_id` |
| Chỉ test not_null | Business assertion: SUM(revenue) các payment type = tổng |
| Không có data lineage | `dbt docs generate` → host S3 |

### Superset — idempotent, cache bust, security

| Vấn đề | Fix |
|---|---|
| Bootstrap tạo duplicate khi chạy lại | Check tồn tại trước khi tạo |
| Cache stale sau pipeline | Gọi API refresh dataset/chart sau materialize |
| Chart SQL không version control | Lưu trong `superset/charts/`, PR review |
| `admin/admin` public | Env var + public user read-only |

### Merge batch + stream

```mermaid
flowchart TD
    BATCH["nyc-silver/batch/trips"] --> BT["Trino: hive.nyc.batch_trips"]
    STREAM["nyc-silver/stream/trips"] --> ST["Trino: hive.nyc.stream_trips"]
    BT --> STG["dbt stg_trips<br/>UNION ALL<br/>+ is_deleted filter"]
    ST --> STG
    STG --> MART["dbt marts"]
```

---

## Chương 5: Implementation Roadmap

```mermaid
gantt
    title Production Hardening Roadmap
    dateFormat  YYYY-MM-DD
    axisFormat  Tuần %W

    section Phase 1 — Foundation
    Pre-ingest validation               :p1, 2026-07-01, 3d
    Quality gates (verify_silver, freshness):p1b, after p1, 4d

    section Phase 2 — Export + Monitor
    verify_postgres, verify_gold        :p2, after p1b, 3d
    Monitor DAG (8 checks @5min)        :p2b, after p2, 5d
    Alert pipeline (Slack + Email)      :p2c, after p2b, 2d

    section Phase 3 — CDC Chain
    Postgres config + WAL monitor       :p3, after p2c, 2d
    Debezium + Kafka config             :p3b, after p3, 3d
    Spark streaming fixes               :p3c, after p3b, 3d

    section Phase 4 — Storage & Query
    Trino resource groups + batch CTAS  :p4, after p3c, 3d
    dbt CI + incremental models         :p4b, after p4, 3d
    Superset idempotent + cache bust    :p4c, after p4b, 2d

    section Phase 5 — Polish
    Anomaly check upgrade               :p5, after p4c, 2d
    Health Dashboard (Superset)         :p5b, after p5, 2d
    Output contracts YAML               :p5c, after p5b, 2d
```

---

## Appendix: Pod Count Summary

| Component | Dev | Production |
|---|---|---|
| Airflow (scheduler + webserver) | 2 pod | 2 pod |
| Airflow Postgres | 1 pod | RDS |
| Postgres Analytics | 1 pod | RDS |
| Postgres CDC | 1 pod | RDS |
| Debezium | 1 pod | 1 pod |
| Kafka | 1 pod (1 broker) | 3 pod (3 broker) |
| MinIO | 1 pod | S3 (managed) |
| Spark Master + Worker | 2 pod | EMR / Glue |
| Trino | 1 pod | 3 pod (1 coord + 2 workers) |
| Superset | 1 pod | 1 pod |
| **Tổng** | **12 pod** | **10 pod + RDS + S3 + EMR** |
