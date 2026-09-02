"""
Ingesta de timelines: el detalle minuto a minuto de cada partida.

El endpoint de timeline es una llamada aparte de la del match, así que
cada partida cuesta un request extra. Con el límite de 100 requests cada
2 minutos de Riot, no se puede traer todo de golpe: esta función procesa
un presupuesto acotado por ejecución y se llama repetidamente hasta
ponerse al día.

No lleva watermark propio. Compara los match_id que hay bajo raw/ contra
los que hay bajo timelines/ y baja la diferencia: es autorreparable, no
guarda estado que pueda desincronizarse, y al terminar el backfill sigue
recogiendo las partidas nuevas sin cambiar nada.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
glue = boto3.client("glue")

BUCKET = os.environ["BUCKET_NAME"]
API_KEY_PARAM = os.environ["API_KEY_PARAM"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
GLUE_TABLE = os.environ["GLUE_TABLE"]
ROUTING_REGION = os.environ.get("ROUTING_REGION", "americas")

# Presupuesto por ejecución. La ingesta de partidas usa la misma key y
# gasta ~12 requests cada 30 minutos; 60 deja margen holgado dentro del
# límite de 100 cada 2 minutos aunque ambas se solapen.
MAX_POR_EJECUCION = int(os.environ.get("MAX_TIMELINES_POR_EJECUCION", "60"))

BASE_URL = f"https://{ROUTING_REGION}.api.riotgames.com"
REQUEST_DELAY_SECONDS = 1.2


class RiotAPIError(RuntimeError):
    pass


def get_api_key() -> str:
    return ssm.get_parameter(Name=API_KEY_PARAM, WithDecryption=True)["Parameter"]["Value"]


def riot_get(path: str, api_key: str) -> dict:
    """
    Igual que en la ingesta: User-Agent propio porque Cloudflare responde
    403 al de urllib por defecto, y reintento respetando Retry-After.
    """
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"X-Riot-Token": api_key, "User-Agent": "lol-pipeline/1.0"},
    )

    for _ in range(3):
        try:
            with urllib.request.urlopen(request, timeout=15) as respuesta:
                return json.loads(respuesta.read())
        except urllib.error.HTTPError as error:
            if error.code == 429:
                espera = int(error.headers.get("Retry-After", 10))
                logger.warning("Rate limit alcanzado, esperando %ss", espera)
                time.sleep(espera)
                continue
            if error.code in (401, 403):
                raise RiotAPIError(
                    f"{error.code}: la API key es inválida o expiró."
                ) from error
            if error.code == 404:
                raise RiotAPIError("404") from error
            raise RiotAPIError(f"HTTP {error.code} en {path}") from error

    raise RiotAPIError(f"Agotados los reintentos para {path}")


def listar_claves(prefijo: str) -> dict[str, str]:
    """Devuelve {match_id: clave S3} para todo lo que hay bajo el prefijo."""
    encontrados: dict[str, str] = {}
    paginador = s3.get_paginator("list_objects_v2")
    for pagina in paginador.paginate(Bucket=BUCKET, Prefix=prefijo):
        for objeto in pagina.get("Contents", []):
            clave = objeto["Key"]
            if clave.endswith(".json"):
                encontrados[clave.rsplit("/", 1)[-1][:-5]] = clave
    return encontrados


def registrar_particion(puuid: str, year: str, month: str, day: str) -> None:
    """Registra la partición en Glue, como hace la ingesta de partidas."""
    valores = [puuid, year, month, day]
    try:
        glue.get_partition(
            DatabaseName=GLUE_DATABASE, TableName=GLUE_TABLE, PartitionValues=valores
        )
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    tabla = glue.get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)["Table"]
    descriptor = dict(tabla["StorageDescriptor"])
    descriptor["Location"] = (
        f"s3://{BUCKET}/timelines/puuid={puuid}"
        f"/year={year}/month={month}/day={day}/"
    )
    try:
        glue.create_partition(
            DatabaseName=GLUE_DATABASE,
            TableName=GLUE_TABLE,
            PartitionInput={"Values": valores, "StorageDescriptor": descriptor},
        )
    except glue.exceptions.AlreadyExistsException:
        pass


def guardar_timeline(clave_destino: str, match_id: str, timeline: dict) -> None:
    registro = {
        "match_id": match_id,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps(timeline, separators=(",", ":")),
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=clave_destino,
        Body=json.dumps(registro, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event, context):
    presupuesto = int((event or {}).get("max_timelines", MAX_POR_EJECUCION))
    if not 1 <= presupuesto <= 500:
        raise ValueError("max_timelines debe estar entre 1 y 500")

    partidas = listar_claves("raw/")
    timelines = listar_claves("timelines/")
    faltantes = sorted(set(partidas) - set(timelines), reverse=True)

    logger.info(
        "%d partidas, %d con timeline, %d pendientes",
        len(partidas), len(timelines), len(faltantes),
    )
    if not faltantes:
        return {"pendientes": 0, "descargados": 0, "completo": True}

    api_key = get_api_key()
    descargados = sin_timeline = 0
    particiones: set[tuple[str, str, str, str]] = set()

    for match_id in faltantes[:presupuesto]:
        # La partición se hereda de la clave de la partida, para que el
        # timeline caiga junto a ella aunque se baje mucho después.
        clave_destino = partidas[match_id].replace("raw/", "timelines/", 1)

        if context and context.get_remaining_time_in_millis() < 20_000:
            logger.info("Cortando por tiempo restante de Lambda")
            break

        try:
            timeline = riot_get(f"/lol/match/v5/matches/{match_id}/timeline", api_key)
        except RiotAPIError as error:
            if str(error) == "404":
                # Partidas viejas o de modos sin timeline: se guarda un
                # marcador vacío para no reintentarlas en cada ejecución.
                guardar_timeline(clave_destino, match_id, {"info": {"frames": []}})
                sin_timeline += 1
                time.sleep(REQUEST_DELAY_SECONDS)
                continue
            raise

        guardar_timeline(clave_destino, match_id, timeline)
        descargados += 1

        partes = dict(
            trozo.split("=", 1)
            for trozo in clave_destino.split("/")[1:-1]
        )
        particiones.add(
            (partes["puuid"], partes["year"], partes["month"], partes["day"])
        )
        time.sleep(REQUEST_DELAY_SECONDS)

    for puuid, year, month, day in particiones:
        registrar_particion(puuid, year, month, day)

    pendientes = len(faltantes) - descargados - sin_timeline
    logger.info(
        "Timelines: %d descargados, %d sin timeline, %d pendientes",
        descargados, sin_timeline, pendientes,
    )
    return {
        "descargados": descargados,
        "sin_timeline": sin_timeline,
        "pendientes": pendientes,
        "completo": pendientes <= 0,
    }
