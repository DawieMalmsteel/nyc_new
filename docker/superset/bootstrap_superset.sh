#!/usr/bin/env bash
# Bootstrap Superset via REST API: Trino DB + dataset + 4 charts + dashboard.
set -euo pipefail

API="http://localhost:8088/api/v1"
USER="admin"
PASS="admin"

login() {
  curl -sf -X POST "$API/security/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$USER\",\"password\":\"$PASS\",\"provider\":\"db\"}" \
    | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['access_token'])"
}

for i in {1..30}; do
  if curl -sf http://localhost:8088/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

TOKEN=$(login)
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

echo "[superset] registering Trino DB"
EXISTING=$(curl -sf "${AUTH[@]}" "$API/database/" \
  | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); m=[r['id'] for r in d['result'] if r['database_name']=='NYC Trino']; print(m[0] if m else '')")

if [ -n "$EXISTING" ]; then
  DB_ID="$EXISTING"
  echo "[superset] database already exists id=$DB_ID"
else
  DB_PAYLOAD='{
    "database_name": "NYC Trino",
    "engine": "trino",
    "configuration_method": "sqlalchemy_form",
    "sqlalchemy_uri": "trino://analytics@trino-coordinator:8080/hive/mart"
  }'
  RESP=$(curl -s "${AUTH[@]}" -X POST "$API/database/" -d "$DB_PAYLOAD")
  DB_ID=$(echo "$RESP" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['id'])")
  echo "[superset] database id=$DB_ID"
fi

# Register dataset: fact_trips as the canonical data source.
echo "[superset] registering dataset fact_trips"
EXISTING_DS=$(curl -sf "${AUTH[@]}" "$API/dataset/" \
  | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); m=[r['id'] for r in d['result'] if r['table_name']=='fact_trips' and r['database']['id']==$DB_ID]; print(m[0] if m else '')")
if [ -n "$EXISTING_DS" ]; then
  DS_ID="$EXISTING_DS"
  echo "[superset] dataset already exists id=$DS_ID"
else
  DS_PAYLOAD="{\"database\":$DB_ID,\"schema\":\"mart\",\"table_name\":\"fact_trips\"}"
  DS_RESP=$(curl -s "${AUTH[@]}" -X POST "$API/dataset/" -d "$DS_PAYLOAD")
  DS_ID=$(echo "$DS_RESP" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['id'])")
  echo "[superset] dataset id=$DS_ID"
fi

# Create 4 charts against the dataset.
add_chart() {
  local name="$1" viz="$2"
  local payload
  payload=$(python3 -c "
import json
print(json.dumps({
    'slice_name': '$name',
    'viz_type': '$viz',
    'datasource_id': $DS_ID,
    'datasource_type': 'table',
    'params': json.dumps({'viz_type': '$viz', 'datasource': '${DS_ID}__table'}),
}))
")
  curl -sf "${AUTH[@]}" -X POST "$API/chart/" -d "$payload" \
    | python3 -c "import json,sys; print(json.loads(sys.stdin.read()).get('id', ''))" || true
}

declare -a CHART_IDS=()
for triple in \
  "trips_per_hour|bar" \
  "top_pickup_zones|table" \
  "borough_revenue|bar" \
  "daily_trips|line"
do
  IFS='|' read -r name viz <<< "$triple"
  cid=$(add_chart "$name" "$viz" || echo "")
  if [ -n "$cid" ]; then
    CHART_IDS+=("$cid")
    echo "[superset] chart '$name' id=$cid"
  else
    echo "[superset] WARN: failed to create chart '$name'"
  fi
done

# Dashboard (idempotent: check by slug before creating).
EXISTING_DASH=$(curl -sf "${AUTH[@]}" "$API/dashboard/" \
  | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); m=[r['id'] for r in d['result'] if r['slug']=='nyc-taxi']; print(m[0] if m else '')")

if [ -n "$EXISTING_DASH" ]; then
  echo "[superset] dashboard already exists id=$EXISTING_DASH"
elif [ ${#CHART_IDS[@]} -gt 0 ]; then
  echo "[superset] creating dashboard"
  DASH_PAYLOAD='{
    "dashboard_title": "NYC Taxi Overview",
    "slug": "nyc-taxi",
    "published": true,
    "position_json": "{}",
    "json_metadata": "{}"
  }'
  DASH_RESP=$(curl -s "${AUTH[@]}" -X POST "$API/dashboard/" -d "$DASH_PAYLOAD")
  DASH_ID=$(echo "$DASH_RESP" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['id'])")
  echo "[superset] dashboard id=$DASH_ID"
fi
echo "[superset] bootstrap complete"
