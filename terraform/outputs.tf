output "bucket_datalake" {
  description = "Bucket donde se almacenan las partidas crudas"
  value       = aws_s3_bucket.raw.bucket
}

output "lambda_function" {
  description = "Nombre de la función de ingesta"
  value       = aws_lambda_function.ingesta.function_name
}

output "glue_database" {
  description = "Base de datos del catálogo para consultar desde Athena"
  value       = aws_glue_catalog_database.lol.name
}

output "athena_workgroup" {
  description = "Workgroup de Athena con límite de escaneo configurado"
  value       = aws_athena_workgroup.lol.name
}

output "siguiente_paso" {
  description = "Comando para cargar la API key tras el despliegue"
  value       = <<-EOT

    Cargar la API key de Riot (no queda en el código ni en el state):

      aws ssm put-parameter \
        --name ${local.api_key_param} \
        --value "RGAPI-tu-key-aqui" \
        --type SecureString \
        --overwrite \
        --region ${var.aws_region}

    Probar la ingesta manualmente:

      aws lambda invoke \
        --function-name ${aws_lambda_function.ingesta.function_name} \
        --region ${var.aws_region} \
        respuesta.json && cat respuesta.json
  EOT
}
