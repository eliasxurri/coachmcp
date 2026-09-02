resource "aws_glue_catalog_database" "lol" {
  name        = replace("${var.project_name}_db", "-", "_")
  description = "Catálogo de partidas de League of Legends"
}

# Tabla definida ESTÁTICAMENTE, sin Glue Crawler.
#
# Decisión de costo: un crawler diario cuesta ~$2.20/mes (cobra por
# DPU-hora con mínimo de 10 minutos por ejecución). Como el esquema de
# la API de Riot es conocido y estable, definirlo aquí lo reduce a $0
# sin perder funcionalidad. La Lambda registra las particiones nuevas.
#
# Se guarda el JSON crudo completo en una sola columna string en vez de
# mapear los ~200 campos del match detail. Razón: en un data lake, la
# capa raw preserva el dato original sin pérdida. La extracción de
# campos se hace en la capa curada (fase 2) o al vuelo con json_extract
# en Athena.
resource "aws_glue_catalog_table" "matches_raw" {
  name          = "matches_raw"
  database_name = aws_glue_catalog_database.lol.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "json"
  }

  # Particionado por jugador y fecha.
  #
  # Athena cobra por TB escaneado. Sin particiones, cada consulta leería
  # todo el bucket. Con este esquema, "mis partidas de septiembre" solo
  # escanea esa carpeta.
  #
  # El orden importa: puuid va primero porque el caso de uso siempre
  # empieza por un jugador concreto.
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
    location      = "s3://${aws_s3_bucket.raw.bucket}/raw/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"

      parameters = {
        "ignore.malformed.json" = "true"
      }
    }

    columns {
      name    = "match_id"
      type    = "string"
      comment = "ID de la partida (ej. LA2_1234567890)"
    }
    columns {
      name    = "game_creation"
      type    = "bigint"
      comment = "Timestamp de creación en milisegundos"
    }
    columns {
      name    = "game_duration"
      type    = "bigint"
      comment = "Duración en segundos"
    }
    columns {
      name = "game_mode"
      type = "string"
    }
    columns {
      name    = "queue_id"
      type    = "int"
      comment = "420=ranked solo, 440=ranked flex, 450=ARAM"
    }
    columns {
      name    = "ingested_at"
      type    = "string"
      comment = "Timestamp ISO-8601 de la ingesta"
    }
    columns {
      name    = "payload"
      type    = "string"
      comment = "JSON crudo completo del match detail de Riot"
    }
  }
}

# Workgroup de Athena con límite de datos escaneados.
#
# Es una red de seguridad de costo: si una consulta mal escrita intentara
# escanear más de 100 MB, Athena la cancela en vez de facturarla.
resource "aws_athena_workgroup" "lol" {
  name = var.project_name

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    bytes_scanned_cutoff_per_query = 104857600 # 100 MB

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    }
  }
}
