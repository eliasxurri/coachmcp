terraform {
  # 1.10 es el mínimo por use_lockfile: antes de esa versión, bloquear el
  # estado en S3 exigía una tabla de DynamoDB aparte.
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Estado remoto en S3.
  #
  # Con estado local el proyecto solo se puede operar desde la máquina que
  # tiene el archivo: clonarlo en otra da un estado vacío, y un apply ahí
  # intentaría recrear los 50 recursos y fallaría a medias contra los que
  # ya existen.
  #
  # El bloque va vacío a propósito (configuración parcial): un backend no
  # admite variables, y el nombre del bucket lleva el ID de cuenta, así
  # que hardcodearlo dejaría el repo inservible para cualquier otro. Cada
  # quien pasa el suyo en backend.hcl, que está en .gitignore:
  #
  #     terraform init -backend-config=backend.hcl
  backend "s3" {}
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
