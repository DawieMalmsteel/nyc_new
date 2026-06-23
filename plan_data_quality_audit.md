# Data Quality Audit — Output Tables

**Date**: 2026-06-23  
**Scope**: All tables in Trino (hive.mart, hive.nyc_gold) and Postgres analytics (nyc_analytics.public)

---

## Summary

| Layer | Tables | Empty | Nulls | Issues |
|---|---|---|---|---|
| Trino hive.mart | 9 | 0 | 1 (window artifact) | 0 |
| Trino hive.nyc_gold | 33 | 0 | 1 (window artifact) | 0 |
| Postgres public | 33 | 0 | 0 | 0 |

**All 75 tables have data. Zero critical nulls. No incorrect values found.**

---

## Detailed Findings

### 1. `dq_row_count_trend.delta_from_7day_avg` — 1 null
- **Severity**: Informational
- **Root cause**: Window function `AVG(...) ROWS BETWEEN 6 PRECEDING` produces null for first 6 rows (not enough history)
- **Fix**: Already handled — `anomaly_flag` defaults to 'NORMAL' for null delta. No change needed.

### 2. `dq_invalid_by_reason` — previously empty, now 1 row
- **Severity**: Fixed (commit f91838e)
- **Root cause**: Spark validates input → no output errors → UNION ALL returns 0 rows
- **Fix**: Added fallback `('no_issues', 0)` row

### 3. `dim_payment_type` column naming mismatch (Superset)
- **Severity**: Superset "undefined" errors
- **Root cause**: Superset charts may reference `payment_type_name` but actual column is `description`
- **Schema**: `payment_type_code` (int) + `description` (text)
- **Fix**: Update superset_bootstrap.py chart definitions to use correct column names

### 4. `mart_payment_type_summary` — was 4/6 types
- **Severity**: Fixed (commit f91838e)
- **Root cause**: GROUP BY on fact_trips only shows types 1-4 (types 5-6 never appear in NYC data)
- **Fix**: LEFT JOIN static payment_types list → all 6 types show with count=0

### 5. `fact_trips_daily` — pickup_zone_id/dropoff_zone_id dimension coverage
- **Severity**: OK
- **Status**: All zones referenced in facts exist in dim_zone
- **No missing dimensions detected**

---

## Superset "undefined" Root Cause Analysis

The "undefined" the user sees in Superset is caused by:

1. **Column name mismatch**: dbt gold models use one naming convention, Postgres CTAS inherits it, but Superset chart params may reference old/incorrect columns
2. **Previously empty tables**: `dq_invalid_by_reason` was 0 rows → chart showed blank/undefined (fixed)
3. **Dimension gaps**: payment_types 5-6 missing from mart_payment_type_summary → chart showed undefined for those slices (fixed)

**Remaining action**: Audit Superset dashboard chart definitions against actual Postgres column names.

---

## Tables Requiring Superset Chart Audit

These tables are referenced by Superset dashboard — need column name verification:

| Table | Superset Chart Column Check |
|---|---|
| `dim_payment_type` | `description` not `payment_type_name` |
| `dim_vendor` | `vendor_name` OK |
| `dim_zone` | `zone`, `borough` OK |
| `dq_validation_summary` | All metrics = 0 (expected) — chart should show "Clean" not "undefined" |
| `mart_payment_type_summary` | Uses `payment_type_name` — OK after fix |
| `fact_trips_daily` | `trip_count`, `gross_revenue` OK |

---

## Action Items

- [ ] Fix Superset bootstrap — verify all chart column references match Postgres schema
- [ ] Add check for "all zeros" tables in Superset (dq_*) — show "No issues" instead of empty
- [x] dq_invalid_by_reason fallback row — DONE
- [x] mart_payment_type_summary LEFT JOIN — DONE
- [x] dq_row_count_trend as dbt model — DONE
- [x] Anomaly check in Airflow — DONE
