#!/usr/bin/env bash
# One-shot: generate kind.yaml + create cluster + load public images.
# No Makefile needed. Run once per machine.
#   bash scripts/cluster_up.sh          # default: kind
#   bash scripts/cluster_up.sh mycluster
set -euo pipefail

CLUSTER="${1:-kind}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- generate kind.yaml from template ---
echo "=== Generating ${REPO_ROOT}/kind.yaml ==="
sed "s|\${PWD}|${REPO_ROOT}|g" "${REPO_ROOT}/kind.yaml.template" > "${REPO_ROOT}/kind.yaml"

# --- create cluster ---
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "=== Cluster '$CLUSTER' already exists, skipping creation ==="
else
  echo "=== Creating kind cluster '$CLUSTER' ==="
  kind create cluster --name "$CLUSTER" --config "${REPO_ROOT}/kind.yaml"
fi

# --- load public images ---
bash "${SCRIPT_DIR}/setup_kind_images.sh" "$CLUSTER"

# --- sync .ivy2 cache to PVC (one-time, for Spark JARs offline) ---
if [ -d "${REPO_ROOT}/.ivy2" ]; then
  echo "=== Syncing .ivy2 cache to PVC ==="
  docker exec kind-worker mkdir -p /mnt/nyc-project/.ivy2 2>/dev/null || true
  tar cf - -C "${REPO_ROOT}" --warning=no-file-changed .ivy2/ \
    | docker exec -i kind-worker tar xf - -C /mnt/nyc-project || true
fi

echo ""
echo "=== Cluster ready. Next: ==="
echo "  skaffold dev --namespace nyc-taxi"
