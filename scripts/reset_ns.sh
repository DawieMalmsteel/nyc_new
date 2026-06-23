#!/usr/bin/env bash
# Full reset: delete namespace + release PVs. Run before skaffold dev if cluster is in bad state.
set -euo pipefail
NS="${1:-nyc-taxi}"
echo "=== Resetting $NS ==="
helm uninstall nyc-taxi -n "$NS" --ignore-not-found 2>/dev/null || true
kubectl delete ns "$NS" --force --grace-period=0 --ignore-not-found 2>/dev/null || true
sleep 1
for pv in $(kubectl get pv -o name 2>/dev/null); do
  claim_ns=$(kubectl get "$pv" -o jsonpath='{.spec.claimRef.namespace}' 2>/dev/null) || true
  if [ "$claim_ns" = "$NS" ]; then
    kubectl patch "$pv" --type json -p '[{"op":"remove","path":"/spec/claimRef"}]' 2>/dev/null || true
  fi
done
echo "=== $NS reset complete ==="
