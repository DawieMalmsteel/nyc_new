# NYC Taxi Pipeline — Updated Architecture with Data Quality Monitoring

> Thiết kế hệ thống monitoring & quality gate. Không sửa code cũ.

---

## Hai luồng song song

```mermaid
flowchart LR
    subgraph MAIN["MAIN FLOW — pipeline chính (chạy monthly)"]
        direction LR
        M1["spark_batch"] --> M2["trino_bootstrap"]
        M2 --> M3["dbt_build"]
        M3 --> M4["gold_export<br/>→ MinIO"]
        M3 --> M5["materialize<br/>→ Postgres"]
        M5 --> M6["superset_bootstrap"]
        M6 --> M7["analytics_check"]
    end

    subgraph MONITOR["MONITOR FLOW — giám sát song song (@hourly, read-only)"]
        direction LR
        N1["check_silver<br/>row count, null, dist"]
        N2["check_gold<br/>30 tables, match"]
        N3["check_postgres<br/>pg = gold"]
        N4["check_superset<br/>charts OK?"]
        N5["check_freshness<br/>data stale?"]
    end

    M1 -.->|"quan sát"| N1
    M4 -.->|"quan sát"| N2
    M5 -.->|"quan sát"| N3
    M6 -.->|"quan sát"| N4
    M7 -.->|"quan sát"| N5

    N1 & N2 & N3 & N4 & N5 --> ALERT["🚨 Slack + Email<br/>nếu FAIL"]

    style MAIN fill:#1a1a2e,stroke:#555,color:#ddd
    style MONITOR fill:#16213e,stroke:#0f3460,color:#ddd
    style ALERT fill:#c00,stroke:#333,color:#fff
```

> **MAIN FLOW**: Chạy pipeline như cũ, không thay đổi gì.
> **MONITOR FLOW**: DAG riêng, chạy mỗi giờ, chỉ SELECT không ghi. Quan sát output từng node. Nếu phát hiện lỗi → Slack + Email.
> **Không can thiệp**: Monitor fail không ảnh hưởng MAIN. MAIN fail không ảnh hưởng Monitor.
> **Đường đứt nét** (`-.->`) = observation only, không phải dependency.

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

## 7. Implementation Priority

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
