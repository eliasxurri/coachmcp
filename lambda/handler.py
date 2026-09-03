"""
Ingesta incremental de partidas de League of Legends hacia S3.

Flujo por jugador:
  1. Resolver el Riot ID (gameName#tagLine) a un PUUID
  2. Pedir la lista de match IDs recientes
  3. Descartar los ya ingeridos (watermark en DynamoDB)
  4. Descargar el detalle de cada partida nueva y escribirlo a S3
  5. Registrar la partición del día en el catálogo de Glue
  6. Actualizar el watermark

Las partidas terminadas son inmutables, así que nunca se reprocesa nada.
Eso mantiene el consumo muy por debajo del rate limit de Riot
(20 req/s y 100 req/2min para development y personal keys).
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
glue = boto3.client("glue")
dynamodb = boto3.resource("dynamodb")

BUCKET = os.environ["BUCKET_NAME"]
WATERMARK_TABLE = os.environ["WATERMARK_TABLE"]
API_KEY_PARAM = os.environ["API_KEY_PARAM"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
GLUE_TABLE = os.environ["GLUE_TABLE"]
ROUTING_REGION = os.environ.get("ROUTING_REGION", "americas")
SUMMONERS = [s.strip() for s in os.environ["SUMMONERS"].split(",") if s.strip()]
MATCHES_PER_RUN = int(os.environ.get("MATCHES_PER_RUN", "10"))
BACKFILL_DEFAULT_COUNT = 80
BACKFILL_MAX_COUNT = 80
INCREMENTAL_LOCK_KEY = "__ingestion_lock__"
BACKFILL_LOCK_KEY = "__backfill_lock__"
LOCK_TTL_SECONDS = 360

BASE_URL = f"https://{ROUTING_REGION}.api.riotgames.com"

# Pausa entre llamadas. El límite real es 20 req/s, pero el cuello de
# botella es el de 100 req cada 2 minutos (~0.83 req/s sostenido).
# 1.2s deja margen cómodo y evita cualquier 429.
REQUEST_DELAY_SECONDS = 1.2


class RiotAPIError(Exception):
    pass


def get_api_key() -> str:
    """Lee la API key desde Parameter Store (SecureString)."""
    response = ssm.get_parameter(Name=API_KEY_PARAM, WithDecryption=True)
    return response["Parameter"]["Value"]


def adquirir_bloqueo(context, lock_key: str = INCREMENTAL_LOCK_KEY) -> str:
    """Adquiere un lock con vencimiento para serializar las ejecuciones."""
    tabla = dynamodb.Table(WATERMARK_TABLE)
    ahora = int(time.time())
    owner = getattr(context, "aws_request_id", None) or f"local-{time.time_ns()}"
    try:
        tabla.put_item(
            Item={
                "puuid": lock_key,
                "owner": owner,
                "expires_at": ahora + LOCK_TTL_SECONDS,
            },
            ConditionExpression="attribute_not_exists(puuid) OR expires_at < :now",
            ExpressionAttributeValues={":now": ahora},
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise RuntimeError("Ya hay otra ejecución de ingesta en curso") from error
        raise
    return owner


def liberar_bloqueo(owner: str, lock_key: str = INCREMENTAL_LOCK_KEY) -> None:
    """Libera solo el lock que pertenece a esta invocación."""
    tabla = dynamodb.Table(WATERMARK_TABLE)
    try:
        tabla.delete_item(
            Key={"puuid": lock_key},
            ConditionExpression="#owner = :owner",
            ExpressionAttributeNames={"#owner": "owner"},
            ExpressionAttributeValues={":owner": owner},
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        logger.warning("El lock de ingesta ya había expirado o cambiado de dueño")


def riot_get(
    path: str, api_key: str, params: dict | None = None, base: str | None = None
) -> dict | list:
    """
    Llama a la API de Riot con reintento ante 429.

    La API devuelve el header Retry-After cuando aplica throttling,
    así que se respeta ese valor en vez de adivinar el backoff.
    """
    url = f"{base or BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    # Cloudflare (delante de la API de Riot) devuelve 403 al User-Agent
    # por defecto de urllib ("Python-urllib/3.x"), sin importar la key.
    request = urllib.request.Request(
        url,
        headers={
            "X-Riot-Token": api_key,
            "User-Agent": "lol-pipeline/1.0",
        },
    )

    for intento in range(3):
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return json.loads(response.read())

        except urllib.error.HTTPError as error:
            if error.code == 429:
                espera = int(error.headers.get("Retry-After", 10))
                logger.warning("Rate limit alcanzado, esperando %ss", espera)
                time.sleep(espera)
                continue

            if error.code == 403:
                raise RiotAPIError(
                    "403 Forbidden: la API key es inválida o expiró. "
                    "Las development keys de Riot duran 24 horas."
                ) from error

            if error.code == 404:
                raise RiotAPIError(f"404 Not Found: {path}") from error

            raise RiotAPIError(f"HTTP {error.code} en {path}") from error

    raise RiotAPIError(f"Agotados los reintentos para {path}")


def resolver_puuid(riot_id: str, api_key: str) -> tuple[str, str, str]:
    """Convierte 'gameName#tagLine' en un PUUID."""
    if "#" not in riot_id:
        raise ValueError(f"Riot ID inválido (falta '#'): {riot_id}")

    game_name, tag_line = riot_id.split("#", 1)
    path = (
        "/riot/account/v1/accounts/by-riot-id/"
        f"{urllib.parse.quote(game_name)}/{urllib.parse.quote(tag_line)}"
    )

    cuenta = riot_get(path, api_key)
    return cuenta["puuid"], cuenta["gameName"], cuenta["tagLine"]


def leer_watermark(puuid: str) -> set[str]:
    """
    Devuelve los match IDs ya ingeridos para este jugador.

    Se guarda el conjunto de IDs recientes en vez de solo el último,
    porque la API no siempre devuelve las partidas en orden estricto
    y así se evita reingerir por desorden.
    """
    tabla = dynamodb.Table(WATERMARK_TABLE)
    respuesta = tabla.get_item(Key={"puuid": puuid})
    item = respuesta.get("Item")

    if not item:
        return set()

    return set(item.get("processed_match_ids", []))


def guardar_watermark(puuid: str, match_ids: set[str]) -> None:
    """
    Persiste los últimos 100 match IDs procesados.

    Con update_item y no put_item: el item del jugador también guarda su
    rango, y un put lo borraría en cada corrida.
    """
    tabla = dynamodb.Table(WATERMARK_TABLE)
    tabla.update_item(
        Key={"puuid": puuid},
        UpdateExpression="SET processed_match_ids = :ids, updated_at = :ahora",
        ExpressionAttributeValues={
            ":ids": sorted(match_ids)[-100:],
            ":ahora": datetime.now(timezone.utc).isoformat(),
        },
    )


def plataforma_de(match_id: str) -> str | None:
    """
    Deriva el host de plataforma del prefijo del match ID.

    League-V4 enruta por plataforma (la2.api.riotgames.com) y no por región
    (americas), que es la que usa match-v5. No hace falta configurarlo: el
    match_id ya lo dice, "LA2_1621285511" -> "la2".
    """
    prefijo, _, resto = match_id.partition("_")
    return prefijo.lower() if prefijo and resto else None


APEX_KEY_PREFIJO = "__apex_"
APEX_REFRESCO_SEGUNDOS = 6 * 3600


def actualizar_cortes_apex(plataforma: str, api_key: str) -> None:
    """
    Guarda el LP mínimo de Grandmaster y Challenger de la región.

    Hace falta porque esas ligas son de tamaño fijo: no se llega a
    Grandmaster acumulando LP hasta un número, se llega desplazando al
    jugador número 500. Sin este dato, cualquier plan de ascenso razona
    sobre una escalera que no existe.

    Se refresca cada 6 horas y no en cada corrida: el corte se mueve lento
    y cada consulta trae las 500 entradas de la liga completa.
    """
    tabla = dynamodb.Table(WATERMARK_TABLE)
    clave = f"{APEX_KEY_PREFIJO}{plataforma}__"
    ahora = int(time.time())

    try:
        actual = tabla.get_item(Key={"puuid": clave}).get("Item") or {}
        if ahora - int(actual.get("actualizado", 0)) < APEX_REFRESCO_SEGUNDOS:
            return
    except ClientError:
        logger.warning("No se pudo leer el corte apex", exc_info=True)
        return

    ligas = {}
    for nombre, ruta in (("grandmaster", "grandmasterleagues"),
                         ("challenger", "challengerleagues")):
        try:
            liga = riot_get(
                f"/lol/league/v4/{ruta}/by-queue/RANKED_SOLO_5x5",
                api_key,
                base=f"https://{plataforma}.api.riotgames.com",
            )
        except (RiotAPIError, urllib.error.URLError):
            logger.warning("No se pudo obtener la liga %s", nombre, exc_info=True)
            continue

        lps = sorted(e["leaguePoints"] for e in liga.get("entries", []))
        if lps:
            ligas[nombre] = {"corte_lp": lps[0], "plazas": len(lps),
                             "mediana_lp": lps[len(lps) // 2]}
        time.sleep(REQUEST_DELAY_SECONDS)

    if not ligas:
        return

    try:
        tabla.update_item(
            Key={"puuid": clave},
            UpdateExpression="SET ligas = :l, actualizado = :ahora",
            ExpressionAttributeValues={":l": ligas, ":ahora": ahora},
        )
    except ClientError:
        logger.warning("No se pudo guardar el corte apex", exc_info=True)
        return

    gm = ligas.get("grandmaster", {})
    logger.info("Corte de Grandmaster en %s: %s LP", plataforma, gm.get("corte_lp"))


def actualizar_rango(puuid: str, plataforma: str, api_key: str) -> None:
    """
    Guarda el rango actual del jugador junto a su watermark.

    Es una llamada por jugador y por corrida, y se resuelve acá y no en el
    servidor MCP a propósito: así la consulta sigue sin depender de la API
    de Riot y funciona aunque la key esté expirada.

    Un fallo acá no puede tumbar la ingesta: el rango es un extra.
    """
    try:
        entradas = riot_get(
            f"/lol/league/v4/entries/by-puuid/{puuid}",
            api_key,
            base=f"https://{plataforma}.api.riotgames.com",
        )
    except (RiotAPIError, urllib.error.URLError):
        logger.warning("No se pudo obtener el rango de %s", puuid[:8], exc_info=True)
        return

    colas = {
        e["queueType"]: {
            "tier": e.get("tier"),
            "division": e.get("rank"),
            "lp": e.get("leaguePoints"),
            "victorias": e.get("wins"),
            "derrotas": e.get("losses"),
        }
        for e in entradas
        if e.get("queueType")
    }
    if not colas:
        return

    try:
        dynamodb.Table(WATERMARK_TABLE).update_item(
            Key={"puuid": puuid},
            UpdateExpression="SET rango = :r, rango_actualizado = :ahora",
            ExpressionAttributeValues={
                ":r": colas,
                ":ahora": datetime.now(timezone.utc).isoformat(),
            },
        )
    except ClientError:
        # No basta con atrapar los fallos de la API de Riot: si la escritura
        # falla, tampoco puede caerse la ingesta de partidas por un extra.
        logger.warning("No se pudo guardar el rango de %s", puuid[:8], exc_info=True)
        return

    solo = colas.get("RANKED_SOLO_5x5", {})
    logger.info("Rango de %s: %s %s", puuid[:8], solo.get("tier"), solo.get("division"))


def guardar_partida(puuid: str, match: dict) -> tuple[str, bool]:
    """
    Escribe una partida en S3 siguiendo el esquema de particionado.

    Layout: raw/puuid=<id>/year=YYYY/month=MM/day=DD/<match_id>.json

    La fecha viene de gameCreation (cuándo se jugó), no de la fecha de
    ingesta: así una partida queda siempre en la partición correcta
    aunque se ingiera con retraso.
    """
    info = match["info"]
    match_id = match["metadata"]["matchId"]

    jugada_en = datetime.fromtimestamp(info["gameCreation"] / 1000, tz=timezone.utc)

    key = (
        f"raw/puuid={puuid}"
        f"/year={jugada_en:%Y}/month={jugada_en:%m}/day={jugada_en:%d}"
        f"/{match_id}.json"
    )

    # Una línea de JSON por objeto: es el formato que espera el JsonSerDe
    # de Athena. Los campos de arriba se extraen para poder filtrar sin
    # parsear el payload completo; el crudo se conserva íntegro.
    registro = {
        "match_id": match_id,
        "game_creation": info["gameCreation"],
        "game_duration": info["gameDuration"],
        "game_mode": info.get("gameMode", ""),
        "queue_id": info.get("queueId", 0),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload": json.dumps(match, separators=(",", ":")),
    }

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(registro, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            IfNoneMatch="*",
        )
        creada = True
    except ClientError as error:
        codigo = error.response.get("Error", {}).get("Code")
        estado = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if codigo != "PreconditionFailed" and estado != 412:
            raise
        creada = False

    return f"{jugada_en:%Y}/{jugada_en:%m}/{jugada_en:%d}", creada


def registrar_particion(puuid: str, fecha: str) -> None:
    """
    Añade la partición al catálogo de Glue si no existe.

    Esto reemplaza al Glue Crawler (~$2.20/mes). Como el esquema es
    conocido y estable, basta con registrar la partición nueva.
    """
    year, month, day = fecha.split("/")
    valores = [puuid, year, month, day]

    try:
        glue.get_partition(
            DatabaseName=GLUE_DATABASE,
            TableName=GLUE_TABLE,
            PartitionValues=valores,
        )
        return  # ya existe
    except glue.exceptions.EntityNotFoundException:
        pass

    tabla = glue.get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)["Table"]
    descriptor = dict(tabla["StorageDescriptor"])
    descriptor["Location"] = (
        f"s3://{BUCKET}/raw/puuid={puuid}/year={year}/month={month}/day={day}/"
    )

    try:
        glue.create_partition(
            DatabaseName=GLUE_DATABASE,
            TableName=GLUE_TABLE,
            PartitionInput={
                "Values": valores,
                "StorageDescriptor": descriptor,
            },
        )
        logger.info("Partición registrada: %s %s", puuid[:8], fecha)
    except glue.exceptions.AlreadyExistsException:
        pass  # otra ejecución concurrente ganó la carrera


def procesar_jugador(
    riot_id: str,
    api_key: str,
    *,
    start: int = 0,
    count: int = 20,
    max_matches: int = MATCHES_PER_RUN,
    actualizar_watermark: bool = True,
) -> dict:
    """Ingiere una página de partidas de un jugador."""
    puuid, game_name, tag_line = resolver_puuid(riot_id, api_key)
    time.sleep(REQUEST_DELAY_SECONDS)

    ya_procesadas = leer_watermark(puuid)

    ids_recientes = riot_get(
        f"/lol/match/v5/matches/by-puuid/{puuid}/ids",
        api_key,
        {"start": start, "count": count},
    )
    time.sleep(REQUEST_DELAY_SECONDS)

    # El rango se refresca aunque no haya partidas nuevas: es lo que suele
    # cambiar entre corridas cuando el jugador no jugó desde la última.
    if actualizar_watermark and ids_recientes:
        plataforma = plataforma_de(ids_recientes[0])
        if plataforma:
            actualizar_rango(puuid, plataforma, api_key)
            time.sleep(REQUEST_DELAY_SECONDS)
            actualizar_cortes_apex(plataforma, api_key)

    ya_conocidas = [m for m in ids_recientes if m in ya_procesadas]
    nuevas = [m for m in ids_recientes if m not in ya_procesadas][:max_matches]

    if not nuevas:
        logger.info("%s#%s: sin partidas nuevas", game_name, tag_line)
        return {
            "jugador": riot_id,
            "ids_obtenidos": len(ids_recientes),
            "nuevas": 0,
            "ya_existentes": len(ya_conocidas),
            "errores": 0,
        }

    ingeridas, ya_existentes, errores = 0, len(ya_conocidas), 0
    particiones = set()
    procesadas_en_esta_ejecucion = set()

    for match_id in nuevas:
        try:
            match = riot_get(f"/lol/match/v5/matches/{match_id}", api_key)
            fecha, creada = guardar_partida(puuid, match)
            particiones.add(fecha)
            procesadas_en_esta_ejecucion.add(match_id)
            if creada:
                ingeridas += 1
            else:
                ya_existentes += 1
        except (RiotAPIError, KeyError) as error:
            logger.error("Error con %s: %s", match_id, error)
            errores += 1

        time.sleep(REQUEST_DELAY_SECONDS)

    for fecha in particiones:
        registrar_particion(puuid, fecha)

    # El watermark se guarda al final: si algo falla a mitad, la próxima
    # ejecución reintenta las que quedaron pendientes.
    if actualizar_watermark and procesadas_en_esta_ejecucion:
        ya_procesadas.update(procesadas_en_esta_ejecucion)
        guardar_watermark(puuid, ya_procesadas)

    logger.info(
        "%s#%s: %d nuevas, %d existentes, %d errores",
        game_name,
        tag_line,
        ingeridas,
        ya_existentes,
        errores,
    )
    return {
        "jugador": riot_id,
        "ids_obtenidos": len(ids_recientes),
        "nuevas": ingeridas,
        "ya_existentes": ya_existentes,
        "errores": errores,
    }


def entero_evento(value, nombre: str) -> int:
    """Valida enteros del evento sin aceptar booleanos como 0/1."""
    if isinstance(value, bool):
        raise ValueError(f"{nombre} debe ser un entero")
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{nombre} debe ser un entero") from error


def validar_evento_backfill(event: dict) -> tuple[str, int, int]:
    """Valida y normaliza la interfaz pública del backfill manual."""
    player = event.get("player")
    if not isinstance(player, str) or not player.strip():
        raise ValueError("player es obligatorio en modo backfill")
    player = player.strip()
    if player not in SUMMONERS:
        raise ValueError("player debe pertenecer a los jugadores configurados en SUMMONERS")

    start = entero_evento(event.get("start", 0), "start")
    count = entero_evento(event.get("count", BACKFILL_DEFAULT_COUNT), "count")
    if start < 0:
        raise ValueError("start debe ser mayor o igual a 0")
    if not 1 <= count <= BACKFILL_MAX_COUNT:
        raise ValueError(f"count debe estar entre 1 y {BACKFILL_MAX_COUNT}")
    return player, start, count


def ejecutar_backfill(event: dict, api_key: str) -> dict:
    player, start, count = validar_evento_backfill(event)
    resultado = procesar_jugador(
        player,
        api_key,
        start=start,
        count=count,
        max_matches=count,
        actualizar_watermark=False,
    )

    errores = resultado["errores"]
    ids_obtenidos = resultado["ids_obtenidos"]
    retry_required = errores > 0
    complete = not retry_required and ids_obtenidos < count
    if retry_required:
        next_start = start
    elif complete:
        next_start = None
    else:
        next_start = start + ids_obtenidos

    return {
        "mode": "backfill",
        "player": player,
        "start": start,
        "count": count,
        "ids_obtenidos": ids_obtenidos,
        "total_ingeridas": resultado["nuevas"],
        "ya_existentes": resultado["ya_existentes"],
        "errores": errores,
        "retry_required": retry_required,
        "next_start": next_start,
        "complete": complete,
    }


def ejecutar_incremental(api_key: str) -> dict:
    resultados = []

    for riot_id in SUMMONERS:
        try:
            resultados.append(procesar_jugador(riot_id, api_key))
        except RiotAPIError as error:
            logger.error("Fallo con %s: %s", riot_id, error)
            resultados.append(
                {
                    "jugador": riot_id,
                    "ids_obtenidos": 0,
                    "nuevas": 0,
                    "ya_existentes": 0,
                    "errores": 1,
                }
            )
        except Exception:
            logger.exception("Error inesperado con %s", riot_id)
            resultados.append(
                {
                    "jugador": riot_id,
                    "ids_obtenidos": 0,
                    "nuevas": 0,
                    "ya_existentes": 0,
                    "errores": 1,
                }
            )

    total = sum(r["nuevas"] for r in resultados)
    errores = sum(r["errores"] for r in resultados)

    logger.info("Ingesta completa: %d partidas, %d errores", total, errores)

    # Si todo falló, se lanza excepción para que la métrica Errors de
    # Lambda se dispare y la alarma de CloudWatch avise.
    if errores and total == 0:
        raise RuntimeError("La ingesta falló para todos los jugadores")

    return {
        "mode": "incremental",
        "total_ingeridas": total,
        "errores": errores,
        "detalle": resultados,
    }


def lambda_handler(event, context):
    event = event or {}
    if not isinstance(event, dict):
        raise ValueError("event debe ser un objeto JSON")

    mode = event.get("mode", "incremental")
    if mode not in ("incremental", "backfill"):
        raise ValueError("mode debe ser 'incremental' o 'backfill'")

    # Validar antes de leer el secreto permite fallar rápido y sin tocar Riot.
    if mode == "backfill":
        validar_evento_backfill(event)

    lock_key = BACKFILL_LOCK_KEY if mode == "backfill" else INCREMENTAL_LOCK_KEY
    lock_owner = adquirir_bloqueo(context, lock_key)
    try:
        api_key = get_api_key()
        if mode == "backfill":
            return ejecutar_backfill(event, api_key)
        return ejecutar_incremental(api_key)
    finally:
        liberar_bloqueo(lock_owner, lock_key)
