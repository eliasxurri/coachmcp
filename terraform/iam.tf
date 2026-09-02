# Trust policy: solo el servicio Lambda puede asumir este rol.
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project_name}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# Permisos de mínimo privilegio: cada statement acota el Resource al ARN
# exacto que la función necesita, no a "*".
data "aws_iam_policy_document" "lambda_permissions" {
  # Escribir logs. El Resource se acota al log group de esta función.
  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.lambda.arn}:*"]
  }

  # Escribir las partidas crudas. Solo PutObject: la función nunca
  # necesita leer ni borrar del data lake.
  statement {
    sid       = "EscribirDataLake"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/*"]
  }

  # Leer y actualizar el watermark.
  statement {
    sid    = "Watermark"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
    ]
    resources = [aws_dynamodb_table.watermark.arn]
  }

  # Leer la API key de Riot desde Parameter Store.
  statement {
    sid       = "LeerApiKey"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter${local.api_key_param}"]
  }

  # Descifrar el SecureString. Sin este permiso, GetParameter con
  # WithDecryption falla: es el patrón AND de KMS (permiso sobre el
  # recurso + permiso sobre la llave).
  statement {
    sid       = "DescifrarApiKey"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = ["arn:aws:kms:${var.aws_region}:${data.aws_caller_identity.current.account_id}:alias/aws/ssm"]
  }

  # Registrar particiones nuevas en el catálogo de Glue.
  # Esto reemplaza al Glue Crawler (~$2.20/mes): como el esquema es
  # conocido y estable, la propia Lambda añade la partición del día.
  statement {
    sid    = "RegistrarParticiones"
    effect = "Allow"
    actions = [
      "glue:GetPartition",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:GetTable",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.lol.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lol.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${var.project_name}-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_permissions.json
}
