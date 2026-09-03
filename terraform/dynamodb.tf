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

# Histórico de LP.
#
# El item del jugador guarda solo su LP actual, así que sobrescribirlo en
# cada corrida borra la información que hace falta para la única pregunta
# que no podíamos responder: cuánto LP se gana por victoria y se pierde por
# derrota. Sin eso, cualquier plan de ascenso descansa en una estimación
# —y el resultado cambia de "alcanzable" a "imposible" según el supuesto.
#
# Una fila por cambio de LP, no por corrida: entre partidas el valor no se
# mueve y guardar repetidos solo agrega ruido.
resource "aws_dynamodb_table" "lp_historico" {
  name         = "${var.project_name}-lp-historico"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "puuid"
  range_key    = "momento"

  attribute {
    name = "puuid"
    type = "S"
  }

  attribute {
    name = "momento"
    type = "N"
  }

  # El histórico se reconstruye solo con el tiempo; no justifica el costo.
  point_in_time_recovery {
    enabled = false
  }
}
