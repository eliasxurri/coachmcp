# Servidor MCP remoto: el envase que permite usar el análisis sin instalar
# nada.
#
# El servidor por stdio exige venv, credenciales de AWS y editar un archivo
# de configuración. Claude.ai acepta conectores remotos en todos sus planes
# pegando una URL, así que esta Lambda expone las mismas 7 herramientas por
# HTTP y las vuelve alcanzables para un jugador cualquiera.
#
# Es infraestructura de validación, no de producción: sirve para poner el
# análisis frente a 10-20 personas y medir si vuelven, antes de gastar
# semanas en multiinquilino de verdad.

resource "aws_cloudwatch_log_group" "mcp" {
  name              = "/aws/lambda/${var.project_name}-mcp"
  retention_in_days = 14
}

# Quién es cada token.
#
# Sin registro ni OAuth: el onboarding del beta es manual a propósito. La
# fila la escribe el operador al dar de alta a alguien, y `ultimo_uso` es la
# señal que decide si el beta funcionó.
resource "aws_dynamodb_table" "usuarios" {
  name         = "${var.project_name}-usuarios"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "token"

  attribute {
    name = "token"
    type = "S"
  }

  point_in_time_recovery {
    enabled = false # se reconstruye dando de alta a los usuarios otra vez
  }
}

# El zip lo arma mcp-server/construir.sh: hay que instalar dependencias, y
# archive_file solo comprime un directorio que ya exista.
data "archive_file" "mcp" {
  type        = "zip"
  source_dir  = "${path.module}/../mcp-server/build"
  output_path = "${path.module}/build/lambda-mcp.zip"

  excludes = ["__pycache__", "*.pyc"]
}

resource "aws_lambda_function" "mcp" {
  function_name = "${var.project_name}-mcp"
  role          = aws_iam_role.mcp.arn

  filename         = data.archive_file.mcp.output_path
  source_code_hash = data.archive_file.mcp.output_base64sha256

  handler = "app.handler"
  runtime = "python3.12"

  # 512 MB no es por memoria sino por CPU: importar pydantic y el SDK de MCP
  # domina el arranque en frío, y más CPU lo acorta.
  memory_size = 512

  # run_query espera hasta 60s por consulta de Athena; el timeout tiene que
  # dejar margen por encima de eso.
  timeout = 90

  environment {
    variables = {
      USERS_TABLE      = aws_dynamodb_table.usuarios.name
      WATERMARK_TABLE  = aws_dynamodb_table.watermark.name
      LP_TABLE         = aws_dynamodb_table.lp_historico.name
      ATHENA_WORKGROUP = aws_athena_workgroup.lol.name
      ATHENA_DATABASE  = aws_glue_catalog_database.lol.name

      # El SDK de MCP rechaza con 421 cualquier Host que no esté declarado.
      ALLOWED_HOSTS = "${aws_apigatewayv2_api.mcp.id}.execute-api.${var.aws_region}.amazonaws.com"
    }
  }

  depends_on = [
    aws_iam_role_policy.mcp,
    aws_cloudwatch_log_group.mcp,
  ]
}

# Entrada pública vía API Gateway (HTTP API).
#
# La primera versión usaba una Lambda Function URL, que es más simple y sale
# gratis. No funcionó: esta cuenta bloquea las URLs públicas de Lambda a
# nivel de cuenta, y devuelve 403 antes de invocar la función. Se comprobó
# creando una Lambda vacía con su propia Function URL pública — también 403,
# con la policy de recurso correcta y sin pertenecer a ninguna organización.
#
# API Gateway no está sujeto a ese control. A escala de beta el costo es
# despreciable (~$1 por millón de peticiones) y no exige tocar ajustes de la
# cuenta. Si más adelante se levanta el bloqueo, volver a Function URL es un
# cambio acotado.
#
# El formato de payload 2.0 es el mismo que emite una Function URL, así que
# Mangum funciona sin cambios en el código.
resource "aws_apigatewayv2_api" "mcp" {
  name          = "${var.project_name}-mcp"
  protocol_type = "HTTP"
  description   = "Servidor MCP remoto para conectores de Claude.ai"
}

resource "aws_apigatewayv2_integration" "mcp" {
  api_id                 = aws_apigatewayv2_api.mcp.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.mcp.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 30000
}

# Ruta atrapatodo: el enrutado real (/u/<token>/mcp) lo hace la aplicación,
# que ya tiene que resolver el token de todos modos.
resource "aws_apigatewayv2_route" "mcp" {
  api_id    = aws_apigatewayv2_api.mcp.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.mcp.id}"
}

resource "aws_apigatewayv2_stage" "mcp" {
  api_id      = aws_apigatewayv2_api.mcp.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "mcp_apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mcp.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.mcp.execution_arn}/*/*"
}

resource "aws_cloudwatch_metric_alarm" "mcp_errores" {
  alarm_name          = "${var.project_name}-errores-mcp"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 5
  treat_missing_data  = "notBreaching"

  alarm_description = "El servidor MCP remoto falló más de 5 veces en una hora"

  dimensions = {
    FunctionName = aws_lambda_function.mcp.function_name
  }
}

resource "aws_iam_role" "mcp" {
  name               = "${var.project_name}-mcp-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# Permisos de solo lectura sobre la capa curada.
#
# El servidor atiende peticiones de internet, así que es la superficie más
# expuesta del proyecto: no toca raw/ ni timelines/, no escribe en el data
# lake y no puede correr en el workgroup de ETL.
data "aws_iam_policy_document" "mcp_permissions" {
  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.mcp.arn}:*"]
  }

  # Solo el workgroup interactivo, con su corte de 100 MB por consulta: es
  # el guardarraíl que protege justo este camino, por donde llegan consultas
  # que las escribe un modelo a partir de lo que pide un usuario.
  statement {
    sid    = "Athena"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
    ]
    resources = [aws_athena_workgroup.lol.arn]
  }

  statement {
    sid    = "GlueSoloLectura"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.lol.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lol.name}/*",
    ]
  }

  statement {
    sid    = "ListarBucket"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.raw.arn, aws_s3_bucket.athena_results.arn]
  }

  # Únicamente la capa curada. El JSON crudo no se expone.
  statement {
    sid     = "LeerCapaCurada"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.raw.arn}/curated/*",
      "${aws_s3_bucket.raw.arn}/peers/*",
      "${aws_s3_bucket.raw.arn}/timeline_frames/*",
    ]
  }

  statement {
    sid    = "ResultadosAthena"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.athena_results.arn}/results/*"]
  }

  statement {
    sid    = "Usuarios"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.usuarios.arn]
  }

  # Solo lectura: el rango lo escribe la ingesta, que es quien habla con
  # la API de Riot. Este servidor nunca escribe en el watermark.
  statement {
    sid       = "LeerRango"
    effect    = "Allow"
    actions   = ["dynamodb:GetItem"]
    resources = [aws_dynamodb_table.watermark.arn]
  }

  statement {
    sid       = "LeerHistoricoLP"
    effect    = "Allow"
    actions   = ["dynamodb:Query"]
    resources = [aws_dynamodb_table.lp_historico.arn]
  }
}

resource "aws_iam_role_policy" "mcp" {
  name   = "${var.project_name}-mcp-policy"
  role   = aws_iam_role.mcp.id
  policy = data.aws_iam_policy_document.mcp_permissions.json
}
