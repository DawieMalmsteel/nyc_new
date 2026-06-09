locals {
  k8s_root = "${path.module}/../k8s"

  # Các kind cluster-scoped không được gắn namespace.
  k8s_cluster_scoped_kinds = toset([
    "Namespace",
    "PersistentVolume",
    "StorageClass",
    "ClusterRole",
    "ClusterRoleBinding",
    "CustomResourceDefinition",
  ])

  k8s_storage_files = sort(tolist(fileset(local.k8s_root, "storage/*.yaml")))

  # Long-running services cho pipeline. Jobs được chạy riêng bên dưới để có thứ tự + wait rõ ràng.
  k8s_application_files = sort(concat(
    tolist(fileset(local.k8s_root, "zookeeper/*.yaml")),
    tolist(fileset(local.k8s_root, "kafka/*.yaml")),
    tolist(fileset(local.k8s_root, "minio/*.yaml")),
    tolist(fileset(local.k8s_root, "kafka-ui/*.yaml")),
    tolist(fileset(local.k8s_root, "spark/*.yaml")),
    tolist(fileset(local.k8s_root, "postgres-cdc/*.yaml")),
    tolist(fileset(local.k8s_root, "debezium/*.yaml")),
    tolist(fileset(local.k8s_root, "trino/*.yaml")),
    tolist(fileset(local.k8s_root, "superset/*.yaml"))
  ))

  k8s_storage_docs = flatten([
    for file_path in local.k8s_storage_files : [
      for doc_index, doc in split("\n---\n", file("${local.k8s_root}/${file_path}")) : {
        key      = "${replace(file_path, "/", "_")}-${doc_index}"
        manifest = yamldecode(doc)
      } if trimspace(doc) != ""
    ]
  ])

  k8s_application_docs = flatten([
    for file_path in local.k8s_application_files : [
      for doc_index, doc in split("\n---\n", file("${local.k8s_root}/${file_path}")) : {
        key      = "${replace(file_path, "/", "_")}-${doc_index}"
        manifest = yamldecode(doc)
      } if trimspace(doc) != ""
    ]
  ])

  k8s_namespace_manifests = {
    namespace = {
      apiVersion = "v1"
      kind       = "Namespace"
      metadata = {
        name = var.k8s_namespace
      }
    }
  }

  k8s_storage_manifests = {
    for item in local.k8s_storage_docs : item.key => merge(item.manifest, {
      metadata = merge(try(item.manifest.metadata, {}), {
        namespace = contains(local.k8s_cluster_scoped_kinds, item.manifest.kind) ? null : var.k8s_namespace
      })
    })
  }

  k8s_application_manifests = {
    for item in local.k8s_application_docs : item.key => merge(item.manifest, {
      metadata = merge(try(item.manifest.metadata, {}), {
        namespace = contains(local.k8s_cluster_scoped_kinds, item.manifest.kind) ? null : var.k8s_namespace
      })
    })
  }
}

resource "kubernetes_manifest" "namespace" {
  for_each = local.k8s_namespace_manifests
  manifest = each.value
}

resource "kubernetes_manifest" "storage" {
  for_each = local.k8s_storage_manifests
  manifest = each.value

  depends_on = [kubernetes_manifest.namespace]
}

resource "kubernetes_manifest" "application" {
  for_each = local.k8s_application_manifests
  manifest = each.value

  depends_on = [kubernetes_manifest.storage]
}

resource "terraform_data" "wait_for_services" {
  count = var.enable_pipeline_jobs ? 1 : 0

  input = var.pipeline_run_id

  depends_on = [kubernetes_manifest.application]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      NS='${var.k8s_namespace}'

      echo '[terraform] waiting for core pods...'
      kubectl wait --for=condition=ready pod -l app=zookeeper -n "$NS" --timeout=180s
      kubectl wait --for=condition=ready pod -l app=kafka -n "$NS" --timeout=180s
      kubectl wait --for=condition=ready pod -l app=minio -n "$NS" --timeout=180s
      kubectl wait --for=condition=ready pod -l app=spark-master -n "$NS" --timeout=120s
      kubectl wait --for=condition=ready pod -l app=spark-worker -n "$NS" --timeout=120s
      kubectl wait --for=condition=ready pod -l app=postgres-cdc -n "$NS" --timeout=180s
      kubectl wait --for=condition=ready pod -l app=debezium -n "$NS" --timeout=180s
      kubectl wait --for=condition=ready pod -l app=trino -n "$NS" --timeout=180s
      kubectl wait --for=condition=ready pod -l app=superset -n "$NS" --timeout=240s
    EOT
  }
}

resource "terraform_data" "run_pipeline" {
  count = var.enable_pipeline_jobs ? 1 : 0

  input = var.pipeline_run_id

  depends_on = [terraform_data.wait_for_services]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      NS='${var.k8s_namespace}'
      K8S_ROOT='${local.k8s_root}'

      apply_wait() {
        local file="$1"
        local job="$2"
        local timeout="$3"
        echo "[terraform] running job/$job from $file"
        kubectl delete job "$job" -n "$NS" --ignore-not-found=true
        kubectl apply -n "$NS" -f "$file"
        kubectl wait --for=condition=complete "job/$job" -n "$NS" --timeout="$timeout"
      }

      echo '[terraform] cleanup old pipeline jobs...'
      kubectl delete job -n "$NS" \
        minio-setup postgres-init topic-init cdc-seed cdc-register cdc-bridge \
        spark-streaming spark-batch spark-batch-m01 spark-batch-m02 spark-batch-m03 \
        trino-bootstrap dbt-build \
        --ignore-not-found=true

      apply_wait "$K8S_ROOT/jobs/minio-setup.yaml" minio-setup 300s

      apply_wait "$K8S_ROOT/jobs/postgres-init.yaml" postgres-init 120s
      apply_wait "$K8S_ROOT/jobs/topic-init.yaml" topic-init 120s

      apply_wait "$K8S_ROOT/jobs/cdc-seed.yaml" cdc-seed 240s
      apply_wait "$K8S_ROOT/jobs/cdc-register.yaml" cdc-register 180s
      apply_wait "$K8S_ROOT/jobs/cdc-bridge.yaml" cdc-bridge 240s
      apply_wait "$K8S_ROOT/jobs/spark-streaming.yaml" spark-streaming 900s

      echo '[terraform] running Spark batch jobs...'
      kubectl apply -n "$NS" -f "$K8S_ROOT/jobs/spark-batch-m01.yaml"
      kubectl apply -n "$NS" -f "$K8S_ROOT/jobs/spark-batch-m02.yaml"
      kubectl apply -n "$NS" -f "$K8S_ROOT/jobs/spark-batch-m03.yaml"
      kubectl wait --for=condition=complete job/spark-batch-m01 -n "$NS" --timeout=900s
      kubectl wait --for=condition=complete job/spark-batch-m02 -n "$NS" --timeout=900s
      kubectl wait --for=condition=complete job/spark-batch-m03 -n "$NS" --timeout=900s

      apply_wait "$K8S_ROOT/jobs/trino-bootstrap.yaml" trino-bootstrap 240s
      apply_wait "$K8S_ROOT/dbt/job.yaml" dbt-build 300s

      echo '[terraform] restarting Superset to run bootstrap against freshly built marts...'
      kubectl rollout restart deployment/superset -n "$NS"
      kubectl rollout status deployment/superset -n "$NS" --timeout=300s

      echo '[terraform] pipeline completed.'
    EOT
  }
}
