# DEPRECATED — use Helm chart instead

This directory is **frozen and stale**. All manifests have been migrated to:

```
charts/nyc-taxi/templates/
```

Key things that are **broken or outdated** in this directory vs Helm:

- Superset: uses `apache/superset:4.0.0` (no trino driver), Flask dev server (no gunicorn)
- Trino: wrong metastore path (`/data/metastore` → `/opt/project/data/trino-metastore`), missing memory limits
- Airflow: no `wait_for_db()`, scheduler uses Deployment (not StatefulSet)
- Jobs: standalone Job YAMLs replaced by Airflow KubernetesPodOperator DAGs

Do not `kubectl apply -f k8s/`. Use `skaffold dev` or `helm install`.
