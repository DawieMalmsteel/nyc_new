terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
  }
}

provider "aws" {
  # MiniStack chạy local trên một gateway endpoint (mặc định: :4566).
  # Dùng access_key 12 chữ số để MiniStack map thành account id riêng cho local test.
  access_key = var.ministack_access_key
  secret_key = var.ministack_secret_key
  region     = var.aws_region

  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_region_validation      = true
  skip_requesting_account_id  = true

  endpoints {
    s3  = var.ministack_endpoint
    sts = var.ministack_endpoint
  }
}

provider "kubernetes" {
  config_path = var.kube_config_path
}
