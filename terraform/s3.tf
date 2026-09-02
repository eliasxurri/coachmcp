resource "aws_s3_bucket" "raw" {
  bucket = local.bucket_name
}

# Bloquear todo acceso público: el data lake nunca debe ser accesible desde internet.
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Cifrado en reposo con la llave gestionada por AWS (aws/s3).
# No se usa una CMK propia porque no hay requisito de control granular
# sobre quién descifra, y una CMK cuesta $1/mes.
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning: protege contra sobrescrituras accidentales durante el desarrollo.
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  # Las partidas terminadas nunca cambian y se consultan cada vez menos
  # con el tiempo: pasado un tiempo se mueven a clases más baratas.
  rule {
    id     = "archivar-partidas-antiguas"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    # 30 días es el mínimo que exige S3 antes de transicionar a IA.
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }
  }

  # Las versiones antiguas (por el versioning) no aportan valor pasada
  # una semana: se eliminan para no pagar almacenamiento duplicado.
  rule {
    id     = "limpiar-versiones-antiguas"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  # Los multipart uploads fallidos quedan facturándose invisiblemente.
  rule {
    id     = "abortar-multipart-incompletos"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Ubicación donde Athena deja los resultados de las consultas.
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${local.bucket_name}-athena-results"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Los resultados de Athena son desechables: se borran a los 7 días.
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expirar-resultados"
    status = "Enabled"

    filter {}

    expiration {
      days = 7
    }
  }
}
