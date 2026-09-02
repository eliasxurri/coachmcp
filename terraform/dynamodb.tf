# Watermark de ingesta incremental.
#
# Las partidas terminadas son inmutables, así que no tiene sentido
# reprocesarlas. Esta tabla guarda, por jugador, cuál fue la última
# partida ya ingerida. Cada ejecución solo pide las nuevas.
#
# Esto es lo que hace que el pipeline quepa holgadamente dentro del
# rate limit de Riot (100 requests / 2 minutos).
resource "aws_dynamodb_table" "watermark" {
  name         = "${var.project_name}-watermark"
  billing_mode = "PAY_PER_REQUEST" # on-demand: el volumen es mínimo e irregular
  hash_key     = "puuid"

  attribute {
    name = "puuid"
    type = "S"
  }

  point_in_time_recovery {
    enabled = false # el estado se puede reconstruir desde S3; no justifica el costo
  }
}
