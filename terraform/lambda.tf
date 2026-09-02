# API key de Riot en Parameter Store como SecureString.
#
# Decisión de costo: Secrets Manager cuesta $0.40/mes por secreto.
# Parameter Store Standard tier es gratis y ofrece lo mismo salvo la
# rotación automática, que aquí no aporta porque la key de Riot no rota
# por sí sola de todos modos.
#
# El valor NO se define en Terraform: se carga aparte con la CLI para
# que la key nunca quede en el código ni en el state file.
#   aws ssm put-parameter --name /lol-pipeline/riot-api-key \
#       --value "RGAPI-..." --type SecureString --overwrite
resource "aws_ssm_parameter" "riot_api_key" {
  name  = local.api_key_param
  type  = "SecureString"
  value = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }
}

# Log group creado explícitamente (en vez de dejar que Lambda lo cree)
# para poder fijarle retención: sin esto los logs se guardan para
# siempre y van acumulando costo.
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}-ingesta"
  retention_in_days = 14
}

data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda"
  output_path = "${path.module}/build/lambda.zip"
}

resource "aws_lambda_function" "ingesta" {
  function_name = "${var.project_name}-ingesta"
  role          = aws_iam_role.lambda.arn

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  handler = "handler.lambda_handler"
  runtime = "python3.12"

  # 256 MB es suficiente: el trabajo es I/O (llamadas HTTP), no cómputo.
  # Subir la memoria daría más CPU pero no aceleraría las esperas de red.
  memory_size = 256

  # Con 10 jugadores x 10 partidas y pausas por rate limit, el peor caso
  # ronda los 2 minutos. 300s da margen sin arriesgar el timeout de 15 min.
  timeout = 300

  environment {
    variables = {
      BUCKET_NAME     = aws_s3_bucket.raw.bucket
      WATERMARK_TABLE = aws_dynamodb_table.watermark.name
      API_KEY_PARAM   = local.api_key_param
      GLUE_DATABASE   = aws_glue_catalog_database.lol.name
      GLUE_TABLE      = aws_glue_catalog_table.matches_raw.name
      ROUTING_REGION  = var.riot_routing_region
      SUMMONERS       = join(",", var.tracked_summoners)
      MATCHES_PER_RUN = tostring(var.matches_per_run)
    }
  }

  depends_on = [
    aws_iam_role_policy.lambda,
    aws_cloudwatch_log_group.lambda,
  ]
}

# Schedule de ingesta.
#
# 30 minutos es deliberado: una partida de LoL dura 25-35 min, así que
# ejecutar más seguido solo gastaría requests sin encontrar datos nuevos.
resource "aws_cloudwatch_event_rule" "schedule" {
  name                = "${var.project_name}-schedule"
  description         = "Dispara la ingesta de partidas"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule = aws_cloudwatch_event_rule.schedule.name
  arn  = aws_lambda_function.ingesta.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingesta.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule.arn
}

# Alarma sobre errores de la función.
#
# El fallo más probable es que la API key expire (las development keys
# de Riot duran 24 horas). Sin esta alarma, el pipeline dejaría de
# ingerir en silencio.
resource "aws_cloudwatch_metric_alarm" "errores" {
  alarm_name          = "${var.project_name}-errores-ingesta"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 2
  treat_missing_data  = "notBreaching"

  alarm_description = "La ingesta falló más de 2 veces en una hora (probable API key expirada)"

  dimensions = {
    FunctionName = aws_lambda_function.ingesta.function_name
  }
}
