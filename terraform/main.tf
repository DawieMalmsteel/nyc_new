# Quản lý S3 buckets trên MiniStack
resource "aws_s3_bucket" "nyc_buckets" {
  for_each = var.enable_ministack_buckets ? var.bucket_names : []
  bucket   = each.value
}
