terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # El nombre del bucket debe ser globalmente único: se le añade el account ID.
  bucket_name = "${var.project_name}-raw-${data.aws_caller_identity.current.account_id}"

  # Ruta del parámetro donde vive la API key de Riot.
  # SecureString en Parameter Store (Standard tier) en vez de Secrets Manager:
  # misma funcionalidad para este caso y $0.40/mes menos.
  api_key_param = "/${var.project_name}/riot-api-key"
}
