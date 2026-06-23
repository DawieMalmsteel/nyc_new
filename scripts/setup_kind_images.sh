#!/usr/bin/env bash
# One-time setup: pull + load all public images into kind nodes.
# Run after kind create cluster OR when images go missing.
# NOT part of skaffold pre-deploy hook — run once, not every deploy.
set -euo pipefail

CLUSTER="${1:-kind}"
NODES=(kind-worker kind-worker2 kind-control-plane)

IMAGES=(
  apache/spark:3.5.1
  postgres:16-alpine
  debezium/connect:2.7.3.Final
  minio/mc:latest
  provectuslabs/kafka-ui:latest
  busybox:1.36
  confluentinc/cp-kafka:7.6.1
  minio/minio:latest
  trinodb/trino:435
  confluentinc/cp-zookeeper:7.6.1
)

echo "=== Pulling ${#IMAGES[@]} public images (linux/amd64) ==="
for img in "${IMAGES[@]}"; do
  if docker image inspect "$img" >/dev/null 2>&1; then
    echo "  $img ... (cached)"
    continue
  fi
  echo "  $img ..."
  # retry up to 3 times for large images
  for i in 1 2 3; do
    docker pull --platform linux/amd64 "$img" && break || {
      echo "    attempt $i failed, retrying..."
      sleep 2
    }
  done
done

echo ""
echo "=== Loading into kind cluster '$CLUSTER' ==="
# ponytail: kind load docker-image is 2-3x faster than docker save + tee
for img in "${IMAGES[@]}"; do
  if docker exec "${NODES[0]}" ctr -n k8s.io images ls -q 2>/dev/null | grep -qF "$img"; then
    echo "  $img (already loaded)"
    continue
  fi
  echo "  $img"
  kind load docker-image "$img" --name "$CLUSTER" 2>&1 || {
    echo "    retrying with fallback..."
    docker save "$img" | docker exec -i "${NODES[0]}" ctr -n k8s.io images import -
  }
done

echo ""
echo "=== Done ==="
