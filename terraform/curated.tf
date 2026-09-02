# Capa curada: matches_raw aplanado a Parquet, una fila por jugador
# rastreado y partida.
#
# Decisión de costo: la transformación la hace Athena (INSERT INTO ...
# SELECT) orquestada por una Lambda mínima. Las alternativas cobran más:
# un Glue Job factura por DPU-hora, y escribir Parquet desde la Lambda
# de ingesta exigiría empaquetar pyarrow (~100 MB). Athena solo cobra lo
# escaneado, que el lookback de la consulta acota a unos MB por corrida.
#
# Se particiona solo por puuid, sin fecha: un INSERT cada 30 minutos
# sobre particiones por día generaría una explosión de archivos Parquet
# diminutos. Con partición por jugador, el filtrado temporal lo resuelven
# las estadísticas min/max de columna del propio Parquet.
resource "aws_glue_catalog_table" "matches_curated" {
  name          = "matches_curated"
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
    location      = "s3://${aws_s3_bucket.raw.bucket}/curated/"
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
      name = "game_name"
      type = "string"
    }
    columns {
      name = "tag_line"
      type = "string"
    }
    columns {
      name    = "jugada_en"
      type    = "timestamp"
      comment = "Cuándo se jugó (de gameCreation), no cuándo se ingirió"
    }
    columns {
      name    = "queue_id"
      type    = "int"
      comment = "420=ranked solo, 440=ranked flex, 450=ARAM"
    }
    columns {
      name = "duracion_min"
      type = "double"
    }
    columns {
      name = "campeon"
      type = "string"
    }
    columns {
      name    = "rol"
      type    = "string"
      comment = "TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY"
    }
    columns {
      name = "victoria"
      type = "boolean"
    }
    columns {
      name = "kills"
      type = "int"
    }
    columns {
      name = "deaths"
      type = "int"
    }
    columns {
      name = "assists"
      type = "int"
    }
    columns {
      name    = "cs"
      type    = "int"
      comment = "Súbditos + monstruos neutrales"
    }
    columns {
      name = "oro"
      type = "int"
    }
    columns {
      name = "dano_a_campeones"
      type = "int"
    }
    columns {
      name = "vision_score"
      type = "int"
    }
    columns {
      name    = "kda"
      type    = "double"
      comment = "(kills+asistencias)/muertes, calculado por Riot"
    }
    columns {
      name    = "participacion_kills"
      type    = "double"
      comment = "Fracción de las kills del equipo en las que participó (0-1)"
    }
    columns {
      name    = "pct_dano_equipo"
      type    = "double"
      comment = "Fracción del daño del equipo que hizo este jugador (0-1)"
    }
    columns {
      name    = "pct_dano_recibido_equipo"
      type    = "double"
      comment = "Fracción del daño recibido por el equipo (0-1)"
    }
    columns {
      name    = "dano_por_min"
      type    = "double"
      comment = "Daño a campeones por minuto"
    }
    columns {
      name = "oro_por_min"
      type = "double"
    }
    columns {
      name = "vision_por_min"
      type = "double"
    }
    columns {
      name    = "cs_primeros_10"
      type    = "int"
      comment = "Súbditos de línea en los primeros 10 minutos"
    }
    columns {
      name = "jungla_cs_antes_10"
      type = "int"
    }
    columns {
      name    = "ventaja_cs_rival"
      type    = "double"
      comment = "Máxima ventaja de CS sobre el oponente directo"
    }
    columns {
      name = "ventaja_nivel_rival"
      type = "int"
    }
    columns {
      name    = "ventaja_vision_rival"
      type    = "double"
      comment = "Puede ser negativa: es una diferencia"
    }
    columns {
      name    = "ventaja_oro_xp_lineas"
      type    = "int"
      comment = "Categórico 0/1/2, no oro crudo"
    }
    columns {
      name    = "ventaja_oro_xp_temprana"
      type    = "int"
      comment = "Categórico 0/1/2"
    }
    columns {
      name = "placas_torre"
      type = "int"
    }
    columns {
      name = "takedowns_primeros_min"
      type = "int"
    }
    columns {
      name    = "takedowns"
      type    = "int"
      comment = "Kills + asistencias"
    }
    columns {
      name = "solo_kills"
      type = "int"
    }
    columns {
      name = "kills_en_inferioridad"
      type = "int"
    }
    columns {
      name = "skillshots_esquivados"
      type = "int"
    }
    columns {
      name = "skillshots_acertados"
      type = "int"
    }
    columns {
      name = "inmovilizaciones"
      type = "int"
    }
    columns {
      name = "kills_cerca_torre_enemiga"
      type = "int"
    }
    columns {
      name = "kills_bajo_torre_propia"
      type = "int"
    }
    columns {
      name = "multikills"
      type = "int"
    }
    columns {
      name    = "muertes_por_campeones"
      type    = "int"
      comment = "Muertes causadas por campeones, no por torres o jungla"
    }
    columns {
      name    = "deficit_kills_max"
      type    = "int"
      comment = "Mayor desventaja de kills que tuvo el equipo"
    }
    columns {
      name = "sobrevivio_hp_baja"
      type = "int"
    }
    columns {
      name = "wards_puestas"
      type = "int"
    }
    columns {
      name = "wards_destruidas"
      type = "int"
    }
    columns {
      name = "wards_control"
      type = "int"
    }
    columns {
      name = "wards_sigilo"
      type = "int"
    }
    columns {
      name = "wards_detectoras"
      type = "int"
    }
    columns {
      name = "wards_takedowns"
      type = "int"
    }
    columns {
      name = "wards_takedowns_antes_20"
      type = "int"
    }
    columns {
      name = "wards_protegidas"
      type = "int"
    }
    columns {
      name = "dragones_takedowns"
      type = "int"
    }
    columns {
      name = "barones_takedowns"
      type = "int"
    }
    columns {
      name = "heraldos_takedowns"
      type = "int"
    }
    columns {
      name = "torres_takedowns"
      type = "int"
    }
    columns {
      name = "torres_destruidas"
      type = "int"
    }
    columns {
      name = "inhibidores_takedowns"
      type = "int"
    }
    columns {
      name = "cangrejos"
      type = "int"
    }
    columns {
      name = "robos_epicos"
      type = "int"
    }
    columns {
      name = "jungla_aliada"
      type = "int"
    }
    columns {
      name    = "jungla_enemiga"
      type    = "int"
      comment = "Campamentos robados al jungla rival"
    }
    columns {
      name = "dano_a_objetivos"
      type = "int"
    }
    columns {
      name = "dano_a_torres"
      type = "int"
    }
    columns {
      name = "primera_sangre"
      type = "boolean"
    }
    columns {
      name = "primera_torre"
      type = "boolean"
    }
    columns {
      name    = "tiempo_muerto_seg"
      type    = "int"
      comment = "Segundos totales muerto: mide pérdida de tempo"
    }
    columns {
      name = "mayor_tiempo_vivo_seg"
      type = "int"
    }
    columns {
      name = "dano_mitigado"
      type = "int"
    }
    columns {
      name = "dano_recibido"
      type = "int"
    }
    columns {
      name = "curacion"
      type = "int"
    }
    columns {
      name = "curacion_aliados"
      type = "int"
    }
    columns {
      name = "escudos_aliados"
      type = "int"
    }
    columns {
      name = "sanacion_efectiva"
      type = "int"
    }
    columns {
      name = "tiempo_cc_seg"
      type = "double"
    }
    columns {
      name = "nivel"
      type = "int"
    }
    columns {
      name = "oro_gastado"
      type = "int"
    }
    columns {
      name = "items_comprados"
      type = "int"
    }
    columns {
      name = "hechizo1"
      type = "int"
    }
    columns {
      name = "hechizo2"
      type = "int"
    }
    columns {
      name    = "posicion_individual"
      type    = "string"
      comment = "Posición inferida por Riot; complementa a rol"
    }
    columns {
      name = "rendicion"
      type = "boolean"
    }
    columns {
      name    = "parche"
      type    = "string"
      comment = "Versión del juego: un cambio de parche explica cambios de rendimiento"
    }
  }
}

resource "aws_cloudwatch_log_group" "curado" {
  name              = "/aws/lambda/${var.project_name}-curado"
  retention_in_days = 14
}

data "archive_file" "curado" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda-curado"
  output_path = "${path.module}/build/lambda-curado.zip"
}

resource "aws_lambda_function" "curado" {
  function_name = "${var.project_name}-curado"
  role          = aws_iam_role.curado.arn

  filename         = data.archive_file.curado.output_path
  source_code_hash = data.archive_file.curado.output_base64sha256

  handler = "handler.lambda_handler"
  runtime = "python3.12"

  # Solo lanza una consulta de Athena y espera: no necesita más memoria.
  memory_size = 128

  # Dos INSERT por ejecución (jugador y baseline de pares). A este
  # volumen tardan segundos; 300s deja margen para backfills completos.
  timeout = 300

  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.etl.name
      ATHENA_DATABASE  = aws_glue_catalog_database.lol.name
      WATERMARK_TABLE  = aws_dynamodb_table.watermark.name
      LOOKBACK_DAYS    = "3"
    }
  }

  depends_on = [
    aws_iam_role_policy.curado,
    aws_cloudwatch_log_group.curado,
  ]
}

# Mismo ritmo que la ingesta. Las corridas sin datos nuevos insertan 0
# filas y no escriben archivos, así que curar seguido no fragmenta el
# Parquet: solo mantiene la capa curada fresca para el servidor MCP.
resource "aws_cloudwatch_event_rule" "curado" {
  name                = "${var.project_name}-curado-schedule"
  description         = "Dispara el curado a Parquet"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "curado" {
  rule = aws_cloudwatch_event_rule.curado.name
  arn  = aws_lambda_function.curado.arn
}

resource "aws_lambda_permission" "curado_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.curado.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.curado.arn
}

resource "aws_cloudwatch_metric_alarm" "curado_errores" {
  alarm_name          = "${var.project_name}-errores-curado"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 2
  treat_missing_data  = "notBreaching"

  alarm_description = "El curado a Parquet falló más de 2 veces en una hora"

  dimensions = {
    FunctionName = aws_lambda_function.curado.function_name
  }
}

# Rol propio para el curado: sus permisos no se mezclan con los de la
# ingesta (que no necesita Athena, y esta función no necesita DynamoDB
# ni la API key).
resource "aws_iam_role" "curado" {
  name               = "${var.project_name}-curado-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "curado_permissions" {
  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.curado.arn}:*"]
  }

  statement {
    sid    = "Lock"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
    ]
    resources = [aws_dynamodb_table.watermark.arn]
  }

  statement {
    sid    = "Athena"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
    ]
    resources = [aws_athena_workgroup.etl.arn]
  }

  # Athena ejecuta con las credenciales de quien lanza la consulta, así
  # que este rol necesita los permisos de Glue y S3 que el INSERT usa
  # por debajo: leer raw, escribir curated y registrar particiones.
  statement {
    sid    = "Glue"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.lol.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lol.name}/*",
    ]
  }

  statement {
    sid    = "LeerRawEscribirCurated"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.raw.arn]
  }

  statement {
    sid     = "LeerRaw"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.raw.arn}/raw/*",
      "${aws_s3_bucket.raw.arn}/timelines/*",
    ]
  }

  statement {
    sid    = "EscribirCurated"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
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
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.athena_results.arn]
  }

  statement {
    sid    = "EscribirResultados"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.athena_results.arn}/results/*"]
  }
}

resource "aws_iam_role_policy" "curado" {
  name   = "${var.project_name}-curado-policy"
  role   = aws_iam_role.curado.id
  policy = data.aws_iam_policy_document.curado_permissions.json
}

# Baseline de pares: los 10 participantes de cada partida.
#
# El matchmaking empareja a esos jugadores al mismo MMR que el jugador
# rastreado, así que la tabla es una muestra de su propio elo. Es lo que
# permite pasar de "cambiaste respecto a tu pasado" a "esto está por
# debajo del nivel al que juegas", sin llamar a ninguna API extra: los
# datos ya venían en el payload de cada partida.
#
# Se particiona por rol porque toda comparación de pares es dentro del
# mismo rol; son 5 particiones y cada consulta lee solo la suya.
resource "aws_glue_catalog_table" "peers_curated" {
  name          = "peers_curated"
  database_name = aws_glue_catalog_database.lol.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  partition_keys {
    name = "rol"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.bucket}/peers/"
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
      name    = "puuid"
      type    = "string"
      comment = "PUUID de cualquiera de los 10 jugadores"
    }
    columns {
      name = "jugada_en"
      type = "timestamp"
    }
    columns {
      name = "queue_id"
      type = "int"
    }
    columns {
      name = "duracion_min"
      type = "double"
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
      name = "kills"
      type = "int"
    }
    columns {
      name = "deaths"
      type = "int"
    }
    columns {
      name = "assists"
      type = "int"
    }
    columns {
      name = "kda"
      type = "double"
    }
    columns {
      name = "participacion_kills"
      type = "double"
    }
    columns {
      name = "pct_dano_equipo"
      type = "double"
    }
    columns {
      name = "cs_por_min"
      type = "double"
    }
    columns {
      name = "dano_por_min"
      type = "double"
    }
    columns {
      name = "oro_por_min"
      type = "double"
    }
    columns {
      name = "vision_por_min"
      type = "double"
    }
    columns {
      name = "cs_primeros_10"
      type = "int"
    }
    columns {
      name = "jungla_cs_antes_10"
      type = "int"
    }
    columns {
      name = "placas_torre"
      type = "int"
    }
    columns {
      name = "ventaja_cs_rival"
      type = "double"
    }
    columns {
      name = "ventaja_vision_rival"
      type = "double"
    }
    columns {
      name = "wards_control"
      type = "int"
    }
    columns {
      name = "wards_destruidas"
      type = "int"
    }
    columns {
      name = "tiempo_muerto_seg"
      type = "int"
    }
    columns {
      name = "solo_kills"
      type = "int"
    }
    columns {
      name = "muertes_por_campeones"
      type = "int"
    }
  }
}
