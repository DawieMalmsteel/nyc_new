#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/raw/yellow_taxi/year=2024/month=01
mkdir -p data/raw/yellow_taxi/year=2024/month=02
mkdir -p data/raw/yellow_taxi/year=2024/month=03
mkdir -p data/lookup

JAN_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
FEB_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet"
MAR_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"
LOOKUP_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

JAN_FILE="data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet"
FEB_FILE="data/raw/yellow_taxi/year=2024/month=02/yellow_tripdata_2024-02.parquet"
MAR_FILE="data/raw/yellow_taxi/year=2024/month=03/yellow_tripdata_2024-03.parquet"
LOOKUP_FILE="data/lookup/taxi_zone_lookup.csv"

download_if_missing() {
  local url="$1"
  local file="$2"
  if [[ -f "$file" ]]; then
    echo "[skip] $file already exists"
  else
    echo "[download] $url -> $file"
    curl -fL "$url" -o "$file"
  fi
}

download_if_missing "$JAN_URL" "$JAN_FILE"
download_if_missing "$FEB_URL" "$FEB_FILE"
download_if_missing "$MAR_URL" "$MAR_FILE"
download_if_missing "$LOOKUP_URL" "$LOOKUP_FILE"

echo "[ok] Data download complete"
