variable "aws_region" {
  description = "Región donde se despliega el pipeline. sa-east-1 (São Paulo) es la más cercana a Chile."
  type        = string
  default     = "sa-east-1"
}

variable "project_name" {
  description = "Prefijo para nombrar todos los recursos."
  type        = string
  default     = "lol-pipeline"
}

variable "riot_routing_region" {
  description = "Routing regional de la API de Riot (americas, europe, asia, sea)."
  type        = string
  default     = "americas"
}

variable "tracked_summoners" {
  description = <<-EOT
    Jugadores a rastrear, en formato Riot ID: "gameName#tagLine".
    Cada uno cuesta 2+N requests por ejecución (N = partidas nuevas).
    Con el límite de 100 req/2min de la dev key, hasta ~10 jugadores es seguro.
  EOT
  type        = list(string)
  default     = ["Elias#000"]
}

variable "matches_per_run" {
  description = "Máximo de partidas nuevas a traer por jugador en cada ejecución."
  type        = number
  default     = 10
}

variable "schedule_expression" {
  description = "Frecuencia de ingesta. 30 min es suficiente: las partidas duran ~25-35 min."
  type        = string
  default     = "rate(30 minutes)"
}
