variable "aws_region" {
  description = "AWS region giả lập dùng cho MiniStack."
  type        = string
  default     = "us-east-1"
}

variable "ministack_endpoint" {
  description = "MiniStack gateway endpoint local. Đổi giá trị này nếu bạn chạy MiniStack bằng GATEWAY_PORT khác 4566."
  type        = string
  default     = "http://localhost:4566"
}

variable "ministack_access_key" {
  description = "Access key cho MiniStack. Nếu là 12 chữ số, MiniStack sẽ dùng chính giá trị này làm account id."
  type        = string
  default     = "000000000000"
}

variable "ministack_secret_key" {
  description = "Secret key giả lập cho MiniStack."
  type        = string
  default     = "test"
  sensitive   = true
}

variable "enable_ministack_buckets" {
  description = "Nếu true, tạo S3 buckets trên MiniStack. K8s pipeline vẫn dùng MinIO nội bộ qua svc-minio."
  type        = bool
  default     = true
}

variable "bucket_names" {
  description = "Danh sách S3 buckets cần tạo trên MiniStack cho pipeline NYC Taxi."
  type        = set(string)
  default     = ["nyc-raw", "nyc-silver", "nyc-quarantine", "nyc-lookup", "nyc-gold"]
}

variable "kube_config_path" {
  description = "Đường dẫn kubeconfig cho Kubernetes provider."
  type        = string
  default     = "~/.kube/config"
}

variable "k8s_namespace" {
  description = "Namespace triển khai pipeline trên Kubernetes/kind."
  type        = string
  default     = "nyc-taxi"
}

variable "enable_pipeline_jobs" {
  description = "Nếu true, terraform apply sẽ chạy các Job theo thứ tự: init → CDC → Spark batch/stream → Trino bootstrap → dbt → Superset restart."
  type        = bool
  default     = true
}

variable "pipeline_run_id" {
  description = "Đổi giá trị này khi muốn Terraform chạy lại toàn bộ pipeline jobs. Ví dụ: -var='pipeline_run_id=run-20260609-01'."
  type        = string
  default     = "initial"
}
