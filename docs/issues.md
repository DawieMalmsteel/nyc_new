# NYC Taxi Pipeline — Failure Mode & Issue Inventory

> Brainstorm toàn bộ failure mode trước khi thiết kế cải thiện.
> Phân loại: 🔴 Critical (mất data) | 🟠 Silent (sai nhưng không biết) | 🟡 Ops (biết lỗi, fix lâu) | 🟢 Preventable (process là được)

---

## 🔴 CRITICAL — Mất data, không recover được

| # | Issue | Layer |
|---|---|---|
| C1 | **S3 bucket `nyc-silver` bị xóa** (accidental delete, sai IAM policy) → toàn bộ silver mất. Pipeline có dựng lại được không? Mất bao lâu? | Storage |
| C2 | **S3 Regional outage** (us-east-1 sập) → pipeline crash hàng loạt. Có multi-region? Fallback thế nào? | Infra |
| C3 | **Postgres analytics không backup** → EC2 die → toàn bộ analytics mất. Recovery từ gold_export CTAS lại mất vài giờ. | Postgres |
| C4 | **Airflow scheduler down 2 ngày không ai biết** → 2 ngày không có pipeline run → data gap. Backfill thế nào? | Airflow |
| C5 | **AWS access key bị lộ** → attacker xóa S3 bucket + snapshot → không recover được. MFA delete đã bật chưa? | Security |
| C6 | **Toàn bộ us-east-1 sập** → pipeline chết hoàn toàn. Multi-region strategy là gì? RPO/RTO target? | Infra |
| C7 | **S3 object bị mã hóa nhầm key / mất KMS key** → không ai đọc được data. Key management thế nào? | Security |
| C8 | **ECR image bị xóa hoặc tag `:latest` bị ghi đè** → pod restart kéo sai image → pipeline chạy code cũ. Immutable tag? | Infra |
| C9 | **Trino metastore file bị corrupt** → Trino không thấy bảng nào. Recovery thế nào nếu dùng file-based metastore? | Trino |

---

## 🟠 SILENT FAILURE — Pipeline success nhưng output sai

### Spark → sai data

| # | Issue | Detection |
|---|---|---|
| S1 | **Spark enrichment join zone lookup sai** → duplicate row hoặc null nhầm → silver có data sai logic. Làm sao phát hiện? | Row count reconciliation |
| S2 | **Spark type cast sai** (`col("fare").cast("double")` trên string → null) → cột biến mất không ai biết | Column aggregate invariant |
| S3 | **Spark crash giữa chừng, restart, `mode("append")`** → duplicate row. Có dedup không? | Row count + checksum |
| S4 | **Spark đọc Parquet corrupt từ S3** (silent data corruption) → Spark vẫn đọc được hàng lỗi. Checksum ở đâu? | File-level checksum |
| S5 | **Spark batch chạy success, output sai logic** (sai công thức enrichment, sai validation rule) → silver sai. Ai phát hiện? | Distribution sanity vs tháng trước |
| S6 | **Spark đọc partition overlap** (glob `year=*/month=*` match trùng file) → row duplicate | Row count reconciliation |
| S7 | **Spark streaming lag 2 tiếng, consumer đọc lại từ offset cũ** → duplicate CDC event. Làm sao biết? | Event dedup key |
| S8 | **Raw input row count ≠ silver + quarantine** → rows biến mất âm thầm. Row count reconciliation? | Input = output + quarantine |
| S9 | **NYC TLC schema thay đổi** (thêm/bỏ/đổi cột) → Spark StructType fail hoặc bỏ cột mới không ai biết | Schema diff alert |

### dbt / Trino → sai data

| # | Issue | Detection |
|---|---|---|
| S10 | **dbt model logic sai nhưng test vẫn pass** — ví dụ `AVG(tip_amount/total_amount)` thay vì `SUM(tip_amount)/SUM(total_amount)` → kết quả khác hẳn. Test generic không bắt được business logic | Cross-verify với raw SQL |
| S11 | **Trino trả về kết quả sai** (type coercion, partition miss, query plan optimize bug) → gold table sai. Làm sao biết? | Row count + aggregate cross-check |
| S12 | **dbt model chạy xong nhưng bị broken** do Trino thay đổi catalog version → lần chạy sau mới phát hiện. Validate sau build? | Post-build smoke query |
| S13 | **Dev đổi dbt model, quên update test** → model sai, test pass. Review process? | Code review requirement |
| S14 | **Gold table row count mismatch** giữa Trino view và Postgres materialize → thiếu rows không ai biết | Row count diff report |
| S15 | **gold_export CTAS 28/30 xong, bảng 29 fail** → DAG retry chạy lại từ đầu → 28 bảng export lại + S3 path conflict | Idempotent CTAS |

### Data Quality / Freshness

| # | Issue | Detection |
|---|---|---|
| S16 | **Validation rule không bắt hết lỗi** — ví dụ `trip_distance > 0` nhưng không check upper bound (500 dặm?) → invalid lọt vào silver | Distribution sanity |
| S17 | **Dữ liệu NYC TLC bản thân nó sai** (GPS lỗi, meter lỗi, tài xế nhập sai) — validation rule nào cũng không bắt được | Cross-reference với external source |
| S18 | **Không phân biệt "không có data mới" với "data missing"** — freshness check báo OK dù tháng mới chưa có file | Freshness check + expected row count |
| S19 | **NYC TLC upload sai file** (file tháng 3 đặt tên tháng 4) → data sai partition | File name vs content cross-check |
| S20 | **NYC TLC ngừng publish 1 tháng** → không ai biết vì pipeline vẫn chạy OK trên data cũ | Source availability monitor |

### Superset → sai data

| # | Issue | Detection |
|---|---|---|
| S21 | **Chart SQL viết tay trong SQL Lab sai** — gõ nhầm `SUM(fare_amount)` thay vì `SUM(total_amount)` → số sai. Ai review? | Cross-verify với Trino |
| S22 | **Dataset reference nhầm bảng cũ** sau khi migrate gold → chart hiện 0 hoặc số cũ | Dataset audit trail |
| S23 | **Filter dashboard sai** — timezone mismatch → thiếu 1 ngày data | Cross-verify với Trino |
| S24 | **Superset cache 1 giờ** — data mới đã có nhưng dashboard hiện số cũ | Cache invalidation trigger |
| S25 | **Dashboard chart không khớp nhau** — chart A nói 1.2M trips, chart B tổng các borough ra 1.15M → ít nhất 1 chart sai | Cross-chart consistency check |
| S26 | **`superset_bootstrap.py` set `position_json` cứng** → chart lệch, chồng lên nhau → user nhìn nhầm số | Dashboard layout review |

---

## 🟡 OPERATIONAL PAIN — Biết lỗi nhưng fix lâu

| # | Issue | Layer |
|---|---|---|
| O1 | **gold_export CTAS 30 bảng chạy 8 phút, 1 bảng fail → retry từ đầu** → lãng phí + S3 path có thể conflict | Gold export |
| O2 | **materialize không atomic** — DROP xong INSERT fail → bảng trắng, Superset lỗi | Postgres |
| O3 | **Pod logs bị xóa** (`is_delete_operator_pod=true`) → không xem được log sau khi task chạy | Airflow |
| O4 | **Trino OOMKilled** giữa query → không rõ bảng nào gây OOM, không auto-split | Trino |
| O5 | **DAG retry 3 lần fail → marked failed** — nhưng lần thứ 4 chạy manual lại pass. Tại sao không auto-retry thêm? | Airflow |
| O6 | **Airflow metadata DB đầy** → scheduler chậm, task queue timeout. Ai monitor DB size? | Airflow |
| O7 | **Nhiều người query Trino cùng lúc** → gold_export chiếm hết memory → query khác timeout. Resource isolation? | Trino |
| O8 | **Superset boot quá chậm** (init DB + import) → pipeline chờ 5 phút | Superset |
| O9 | **EKS node bị evict do spot instance reclaim** → Spark job bị kill giữa chừng → output corrupt | Infra |
| O10 | **Kafka consumer group reset** → Spark streaming đọc lại từ đầu → duplicate toàn bộ | CDC |
| O11 | **Skaffold sync không hoạt động** → code cũ chạy trên pod → debug 30 phút mới biết | Deploy |
| O12 | **Không biết pipeline chạy tới đâu** — phải vào Airflow UI, không có status page đơn giản | Observability |

---

## 🟢 PREVENTABLE — Có process là tránh được

| # | Issue | Fix |
|---|---|---|
| P1 | **Dev chạy nhầm `kubectl delete namespace nyc-taxi`** thay vì staging → production bay màu | RBAC + confirmation prompt |
| P2 | **Dev merge PR 5PM thứ 6** → pipeline fail 6PM → không ai biết đến thứ 2 | Deploy window policy |
| P3 | **New team member không biết kiến trúc** → sửa Spark → xóa validation rule → invalid data vào silver | Onboarding doc |
| P4 | **Superset public internet không VPN** → brute-force password → attacker xem data | Rate limiting + VPN |
| P5 | **Không có incident runbook** — ai cũng restart thử, không root-cause | Viết runbook |
| P6 | **Không có code review cho SQL Superset** — chart SQL sai không ai biết | SQL review process |
| P7 | **S3 Intelligent Tiering không bật** → data 2 năm vẫn Standard class → tiền S3 gấp 3 | Lifecycle policy |
| P8 | **Cost không ai kiểm soát** — AWS bill tăng gấp đôi không ai biết tại sao | Budget alert + cost tag |

---

## 📊 Bảng điểm — thứ tự ưu tiên brainstorm

```
Priority = (Impact × Likelihood) + Blocking

Tier 1 (17 điểm trở lên): Phải giải quyết trước khi claim "production-ready"
Tier 2 (12-16 điểm):   Nên có trong 4 tuần đầu production
Tier 3 (dưới 12 điểm):   Có thì tốt, không có vẫn sống được
```

| # | Issue | Impact | Likelihood | Detectability | Priority |
|---|---|---|---|---|---|
| C1 | S3 bucket bị xóa | 10 | 3 | 1 (không biết đến khi query fail) | 🔴 30 |
| C4 | Airflow scheduler down | 8 | 5 | 3 (vào UI mới biết) | 🔴 35 |
| S5 | Spark output sai logic | 9 | 4 | 1 (không biết) | 🔴 36 |
| S21 | Superset chart SQL sai | 7 | 6 | 2 (user report mới biết) | 🔴 35 |
| S18 | Không phân biệt no-data vs missing | 8 | 5 | 1 (không biết) | 🔴 40 |
| S1 | Enrichment join sai | 9 | 4 | 1 (không biết) | 🔴 36 |
| O3 | Pod logs bị xóa | 4 | 8 | 5 (biết ngay) | 28 |
| O2 | Materialize không atomic | 6 | 6 | 3 | 30 |
| S25 | Chart không khớp nhau | 5 | 7 | 4 | 28 |
| C3 | Postgres không backup | 8 | 4 | 2 | 28 |
| S10 | dbt model logic sai | 7 | 5 | 2 | 28 |
| S15 | gold_export retry conflict | 5 | 6 | 3 | 25 |
| S3 | Spark crash duplicate | 6 | 5 | 3 | 25 |
| O4 | Trino OOMKilled | 6 | 6 | 3 | 30 |
| O13 | Không có status page | 3 | 9 | 5 | 22 |
| P4 | Superset public internet | 7 | 4 | 6 | 22 |

---

## 🎯 Nhóm brainstorm theo chủ đề

### Nhóm A: Data Correctness (làm sao biết data đúng hay sai?)

> S1, S2, S3, S5, S6, S8, S10, S14, S16, S18, S21, S22, S23, S24, S25

### Nhóm B: Disaster Recovery (lỡ AWS sập, S3 mất?)

> C1, C2, C3, C6, C7, C9

### Nhóm C: Pipeline Reliability (làm sao pipeline đừng fail vô ích?)

> S4, S9, S19, S20, O1, O2, O4, O5, O9, O10, O11, O12

### Nhóm D: Observability (làm sao biết pipeline đang chạy hay đã chết?)

> O3, O6, O13, C4, S15

### Nhóm E: Governance (ai kiểm soát code, cost, access?)

> P1, P2, P3, P5, P6, P7, P8, P4, C5, C8

---

## Status

- [ ] Nhóm A — Data Correctness
- [ ] Nhóm B — Disaster Recovery  
- [ ] Nhóm C — Pipeline Reliability
- [ ] Nhóm D — Observability
- [ ] Nhóm E — Governance
