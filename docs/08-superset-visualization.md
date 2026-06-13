# 8. Apache Superset — Dashboard và Visualization

## 8.1 Tổng quan

Apache Superset 4.0.0 cung cấp giao diện trực quan cho dữ liệu NYC Taxi, 
kết nối đến Trino để query các mart views.

### Cấu hình kết nối
- **Database**: Trino (SQLAlchemy URI: `trino://analytics@svc-trino:8080/hive/mart`)
- **Schema**: `hive.mart` (dbt views)
- **Thông tin**: Truy cập qua `http://localhost:39080` (K8s) hoặc `http://localhost:8088` (Docker)
- **Credentials**: `admin` / `admin`

---

## 8.2 Bootstrap Script

**Script**: `scripts/superset_bootstrap.py`

Script idempotent tự động:
1. Đăng nhập Superset (REST API)
2. Tạo/kiểm tra Database connection đến Trino
3. Tạo/kiểm tra 7 datasets
4. Tạo/kiểm tra 4 charts
5. Tạo/kiểm tra 1 dashboard

### Chi tiết bootstrap

```python
BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088") + "/api/v1"
TRINO_URI = os.environ.get("TRINO_URI", 
    "trino://analytics@trino-coordinator:8080/hive/mart")
```

### 1. Database
```python
dbs = get("/database/")
# Tìm "NYC Trino" hoặc tạo mới
resp = post("/database/", {
    "database_name": "NYC Trino",
    "sqlalchemy_uri": TRINO_URI
})
```

### 2. Datasets (7 tables từ hive.mart)

| Dataset | Table | Ghi chú |
|---------|-------|---------|
| fact_trips | `hive.mart.fact_trips` | Fact table chính |
| dim_zone | `hive.mart.dim_zone` | Zone dimension |
| mart_hourly_summary | `hive.mart.mart_hourly_summary` | Hourly aggregation |
| mart_payment_type_summary | `hive.mart.mart_payment_type_summary` | Payment type summary |
| mart_revenue_by_day | `hive.mart.mart_revenue_by_day` | Daily revenue |
| mart_revenue_by_zone | `hive.mart.mart_revenue_by_zone` | Revenue by zone |
| gold_fact_trips | `hive.mart.gold_fact_trips` | Gold fact table |

### 3. Charts (4 charts)

| Chart | Viz Type | Dataset | Mô tả |
|-------|----------|---------|-------|
| `trips_per_hour` | bar | fact_trips | Số chuyến theo giờ |
| `top_pickup_zones` | table | fact_trips | Top zones đón khách |
| `borough_revenue` | bar | fact_trips | Doanh thu theo borough |
| `daily_trips` | line | fact_trips | Xu hướng chuyến đi theo ngày |

### 4. Dashboard
- **Title**: "NYC Taxi Overview"
- **Slug**: `nyc-taxi`
- **Published**: true

---

## 8.3 Superset Entrypoint

**File**: `docker/superset/entrypoint-superset.sh`

```bash
# 1. Wait for Trino ready
for i in {1..60}; do
  if curl -sf http://trino-coordinator:8080/v1/info >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# 2. Install SQLAlchemy Trino driver
pip install sqlalchemy-trino -q

# 3. Initialize DB + create admin user
superset db upgrade
superset fab create-admin \
  --username admin --password admin --role Admin

# 4. Init roles + permissions
superset init

# 5. Start webserver (background) + bootstrap
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger &
# Wait for webserver ready, then register DB/charts/dashboard
bash /app/docker/bootstrap_superset.sh
```

### Superset Config

**File**: `docker/superset/superset_config.py`
```python
# Disable CSRF cho bootstrap script (POST requests)
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
ENABLE_CSP = False
```

---

## 8.4 Bootstrap Script

### Kubernetes (Airflow DAG) ⭐

Airflow tự động chạy Superset bootstrap trong `nyc_e2e_pipeline` và `nyc_analytics_refresh`:

```python
KubernetesPodOperator(
    image="nyc-pipeline-tools:k8s",
    cmds=["python3"],
    arguments=["/opt/project/scripts/superset_bootstrap.py"],
    env_vars=[
        ("SUPERSET_URL", "http://svc-superset:8088"),
        ("TRINO_URI", "trino://analytics@svc-trino:8080/hive/mart"),
    ],
)
```

### Docker Compose (Legacy)
```bash
make superset-bootstrap
# Thực thi: docker exec -i nyc_superset python3 < scripts/superset_bootstrap.py
```

---

## 8.5 Analytics Questions (SQL)

**File**: `sql/analytics_questions.sql` — 10 business questions chạy trên Trino.

**Script**: `scripts/run_analytics_questions.py`

### Danh sách 10 câu hỏi

| # | Câu hỏi | SQL |
|---|---------|-----|
| 1 | Top 10 pickup zones by trips | `SELECT pickup_zone, COUNT(*) AS n FROM hive.mart.fact_trips GROUP BY ... ORDER BY n DESC LIMIT 10` |
| 2 | Hourly distribution | `SELECT pickup_hour, COUNT(*), AVG(total_amount) FROM hive.mart.fact_trips GROUP BY pickup_hour` |
| 3 | Borough-to-borough matrix | `SELECT pickup_borough, dropoff_borough, COUNT(*) FROM hive.mart.fact_trips ... LIMIT 20` |
| 4 | Average fare by payment type | `SELECT payment_type, COUNT(*), AVG(fare_amount) FROM hive.mart.fact_trips GROUP BY payment_type` |
| 5 | Daily gross revenue | `SELECT pickup_date, COUNT(*), SUM(total_amount) FROM hive.mart.fact_trips GROUP BY pickup_date` |
| 6 | Top 10 longest trips | `SELECT pickup_ts, trip_distance, total_amount, ... FROM hive.mart.fact_trips ORDER BY trip_distance DESC LIMIT 10` |
| 7 | Top 5 boroughs by revenue | `SELECT pickup_borough, SUM(total_amount), AVG(tip_rate) FROM hive.mart.fact_trips ... LIMIT 5` |
| 8 | Hourly summary via mart | `SELECT * FROM hive.mart.mart_hourly_summary ORDER BY pickup_date, pickup_hour` |
| 9 | Mart inventory | `SELECT table_name, table_type FROM hive.information_schema.tables WHERE table_schema = 'mart'` |
| 10 | Invalid trips view check | `SELECT 'invalid_trips_view_resolves', COUNT(*) FROM hive.mart.fact_invalid_trips` |

### Validation

Script kiểm tra mỗi query trả về ≥1 row. Kỳ vọng **10/10 PASS**.

```python
def main():
    questions = split_questions(raw_sql)
    for i, q in enumerate(questions, 1):
        cur.execute(q)
        rows = cur.fetchall()
        n = len(rows)
        if n == 0:
            failures.append((i, "zero rows"))
    if failures:
        return 1  # FAIL
    return 0  # PASS (10/10)
```

**Output mẫu:**
```
[analytics] 10 questions found in analytics_questions.sql
[Q1] 2573 rows in 1.23s | first: ('Manhattan', 2573)
[Q2]   24 rows in 0.45s | first: (0, 123, 45.67)
...
[analytics] PASS 10/10
```

---

## 8.6 UIs & Port-forwards

### Kubernetes (Skaffold) ⭐

| Service | URL | Credentials |
|---------|-----|-------------|
| **Superset** | http://localhost:39080 | `admin` / `admin` |
| MinIO Console | http://localhost:39086 | `minio` / `minio123` |
| Kafka UI | http://localhost:39082 | — |
| Spark Master | http://localhost:39083 | — |
| Trino | http://localhost:39084 | — |
| Airflow | http://localhost:39085 | `admin` / `admin` |

Port-forward tự động qua Skaffold. Nếu cần thủ công:
```bash
./scripts/k8s_ui.sh start
```

### Docker Compose (Legacy)
| Service | URL | Credentials |
|---------|-----|-------------|
| Superset | http://localhost:8088 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minio` / `minio123` |
| Airflow | http://localhost:8085 | `admin` / `admin` |
| Kafka UI | http://localhost:8080 | — |
