# Timelines: el detalle minuto a minuto de cada partida.
#
# Es el único dato del roadmap que exige llamadas nuevas a la API. Se
# asumió el costo a conciencia: un request extra por partida (el doble
# que antes) y ~1,1 MB por timeline contra 84 KB del resumen. A cambio
# aparece todo lo que el resumen no puede dar: la curva de oro y CS
# contra el rival de línea minuto a minuto, y de ahí los benchmarks de
# fase de líneas (CS al 10, diferencia de oro al 14) que son la base del
# coaching de laneo.
#
# Por el tamaño, consultar estos JSON crudos reventaría el corte de
# 100 MB del workgroup: por eso existe timeline_frames más abajo, que
# los proyecta a Parquet aplanado.
resource "aws_glue_catalog_table" "timelines_raw" {
  name          = "timelines_raw"
  database_name = aws_glue_catalog_database.lol.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "json"
  }

  # Mismo particionado que matches_raw: el timeline se guarda junto a su
  # partida aunque se descargue mucho después.
  partition_keys {
    name = "puuid"
    type = "string"
  }
  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.bucket}/timelines/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "true"
      }
    }

    columns {
      name = "match_id"
      type = "string"
    }
    columns {
      name = "ingested_at"
      type = "string"
    }
    columns {
      name    = "payload"
      type    = "string"
      comment = "JSON crudo completo del timeline de Riot"
    }
  }
}

resource "aws_cloudwatch_log_group" "timeline" {
  name              = "/aws/lambda/${var.project_name}-timeline"
  retention_in_days = 14
}

data "archive_file" "timeline" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda-timeline"
  output_path = "${path.module}/build/lambda-timeline.zip"
}

resource "aws_lambda_function" "timeline" {
  function_name = "${var.project_name}-timeline"
  role          = aws_iam_role.timeline.arn

  filename         = data.archive_file.timeline.output_path
  source_code_hash = data.archive_file.timeline.output_base64sha256

  handler = "handler.lambda_handler"
  runtime = "python3.12"

  # El trabajo es I/O y los timelines se procesan de a uno, así que la
  # memoria no acelera nada.
  memory_size = 256

  # 60 descargas con pausa de 1,2s son ~75s; 300s deja margen para
  # esperas por rate limit sin acercarse al máximo de 15 minutos.
  timeout = 300

  environment {
    variables = {
      BUCKET_NAME    = aws_s3_bucket.raw.bucket
      API_KEY_PARAM  = local.api_key_param
      GLUE_DATABASE  = aws_glue_catalog_database.lol.name
      GLUE_TABLE     = aws_glue_catalog_table.timelines_raw.name
      ROUTING_REGION = var.riot_routing_region
    }
  }

  depends_on = [
    aws_iam_role_policy.timeline,
    aws_cloudwatch_log_group.timeline,
  ]
}

# Desfasado 15 minutos respecto de la ingesta de partidas.
#
# Ambas funciones usan la misma API key y comparten el límite de 100
# requests cada 2 minutos. Alternarlas evita que los picos coincidan.
resource "aws_cloudwatch_event_rule" "timeline" {
  name                = "${var.project_name}-timeline-schedule"
  description         = "Descarga timelines pendientes"
  schedule_expression = "cron(15,45 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "timeline" {
  rule = aws_cloudwatch_event_rule.timeline.name
  arn  = aws_lambda_function.timeline.arn
}

resource "aws_lambda_permission" "timeline_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.timeline.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.timeline.arn
}

resource "aws_cloudwatch_metric_alarm" "timeline_errores" {
  alarm_name          = "${var.project_name}-errores-timeline"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 2
  treat_missing_data  = "notBreaching"

  alarm_description = "La descarga de timelines falló más de 2 veces en una hora"

  dimensions = {
    FunctionName = aws_lambda_function.timeline.function_name
  }
}

resource "aws_iam_role" "timeline" {
  name               = "${var.project_name}-timeline-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "timeline_permissions" {
  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.timeline.arn}:*"]
  }

  # Necesita listar raw/ y timelines/ para saber qué falta: es lo que
  # reemplaza a un watermark propio.
  statement {
    sid       = "ListarBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "timelines/*"]
    }
  }

  statement {
    sid       = "EscribirTimelines"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/timelines/*"]
  }

  statement {
    sid       = "LeerApiKey"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter${local.api_key_param}"]
  }

  statement {
    sid       = "DescifrarApiKey"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = ["arn:aws:kms:${var.aws_region}:${data.aws_caller_identity.current.account_id}:alias/aws/ssm"]
  }

  statement {
    sid    = "RegistrarParticiones"
    effect = "Allow"
    actions = [
      "glue:GetPartition",
      "glue:CreatePartition",
      "glue:GetTable",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.lol.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lol.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "timeline" {
  name   = "${var.project_name}-timeline-policy"
  role   = aws_iam_role.timeline.id
  policy = data.aws_iam_policy_document.timeline_permissions.json
}

# Workgroup aparte para los trabajos de curado.
#
# El workgroup interactivo corta a 100 MB por consulta y esa red de
# seguridad tiene que seguir ahí: las consultas del asistente van contra
# Parquet y nunca deberían acercarse. Pero curar los timelines exige leer
# JSON crudo —unos 840 MB para el histórico completo— y bajo ese límite
# el trabajo legítimo fallaría.
#
# Separarlos deja cada uno con el límite que le corresponde en vez de
# aflojar el guardarraíl para todos. Aun con 5 GB de tope, un backfill
# completo cuesta menos de un centavo.
resource "aws_athena_workgroup" "etl" {
  name = "${var.project_name}-etl"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    bytes_scanned_cutoff_per_query = 5368709120 # 5 GB

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    }
  }
}

# Proyección curada: una fila por minuto de partida, con el jugador
# rastreado y su rival directo de línea lado a lado.
#
# Comparar contra el rival de la misma posición es lo que convierte el
# timeline en algo accionable: "voy 300 de oro abajo en el minuto 10"
# dice mucho más que el oro absoluto, que depende del campeón y del
# ritmo de la partida.
resource "aws_glue_catalog_table" "timeline_frames" {
  name          = "timeline_frames"
  database_name = aws_glue_catalog_database.lol.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  partition_keys {
    name = "puuid"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.bucket}/timeline_frames/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "match_id"
      type = "string"
    }
    columns {
      name    = "minuto"
      type    = "int"
      comment = "Minuto de partida; el timeline trae un frame por minuto"
    }
    columns {
      name = "rol"
      type = "string"
    }
    columns {
      name = "campeon"
      type = "string"
    }
    columns {
      name = "victoria"
      type = "boolean"
    }
    columns {
      name = "oro"
      type = "int"
    }
    columns {
      name = "xp"
      type = "int"
    }
    columns {
      name    = "cs"
      type    = "int"
      comment = "Súbditos + monstruos neutrales acumulados a ese minuto"
    }
    columns {
      name = "nivel"
      type = "int"
    }
    columns {
      name    = "oro_rival"
      type    = "int"
      comment = "NULL si no hay rival directo en esa posición"
    }
    columns {
      name = "xp_rival"
      type = "int"
    }
    columns {
      name = "cs_rival"
      type = "int"
    }
    columns {
      name = "nivel_rival"
      type = "int"
    }
    columns {
      name    = "diff_oro"
      type    = "int"
      comment = "Positivo = por delante del rival de línea"
    }
    columns {
      name = "diff_xp"
      type = "int"
    }
    columns {
      name = "diff_cs"
      type = "int"
    }
  }
}
