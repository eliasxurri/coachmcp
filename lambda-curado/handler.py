"""
Curado: aplana las partidas crudas a la tabla Parquet matches_curated.

La transformación la hace Athena con un INSERT INTO ... SELECT: escribir
Parquet desde Lambda exigiría empaquetar pyarrow (~100 MB) y un Glue Job
cobraría por DPU-hora. Athena ya sabe escribir Parquet y solo cobra por
lo escaneado, así que esta función se limita a lanzar la consulta y
esperar a que termine.

Idempotencia: el anti-join sobre (match_id, puuid) descarta lo ya curado, y el
lookback de pocos días acota cuánto de la capa raw se escanea en cada
ejecución (lo más viejo ya quedó curado en ejecuciones anteriores).
"""

import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client("athena")
dynamodb = boto3.resource("dynamodb")

WORKGROUP = os.environ["ATHENA_WORKGROUP"]
DATABASE = os.environ["ATHENA_DATABASE"]
WATERMARK_TABLE = os.environ["WATERMARK_TABLE"]
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "3"))
LOCK_KEY = "__curated_lock__"
LOCK_TTL_SECONDS = 180

# Una fila por jugador rastreado y partida, con los campos que usan las
# herramientas del MCP ya extraídos. El orden de columnas debe calzar con
# la tabla destino, con la columna de partición (puuid) al final.
SQL_TEMPLATE = """
INSERT INTO matches_curated
WITH nuevos AS (
    SELECT
        r.match_id,
        r.game_creation,
        r.game_duration,
        r.queue_id,
        r.puuid,
        json_extract_scalar(r.payload, '$.info.gameVersion') AS parche,
        p.participante
    FROM matches_raw r
    CROSS JOIN UNNEST(
        CAST(json_extract(r.payload, '$.info.participants') AS ARRAY(JSON))
    ) AS p(participante)
    WHERE json_extract_scalar(p.participante, '$.puuid') = r.puuid
      AND r.year || r.month || r.day >=
          date_format(date_add('day', -{lookback}, current_date), '%Y%m%d')
      AND NOT EXISTS (
          SELECT 1
          FROM matches_curated c
          WHERE c.match_id = r.match_id
            AND c.puuid = r.puuid
      )
)
SELECT
    match_id,
    json_extract_scalar(participante, '$.riotIdGameName') AS game_name,
    json_extract_scalar(participante, '$.riotIdTagline')  AS tag_line,
    from_unixtime(game_creation / 1000) AS jugada_en,
    CAST(queue_id AS INTEGER) AS queue_id,
    round(game_duration / 60.0, 1) AS duracion_min,
    json_extract_scalar(participante, '$.championName') AS campeon,
    json_extract_scalar(participante, '$.teamPosition') AS rol,
    CAST(json_extract_scalar(participante, '$.win') AS BOOLEAN) AS victoria,
    CAST(json_extract_scalar(participante, '$.kills')   AS INTEGER) AS kills,
    CAST(json_extract_scalar(participante, '$.deaths')  AS INTEGER) AS deaths,
    CAST(json_extract_scalar(participante, '$.assists') AS INTEGER) AS assists,
    CAST(json_extract_scalar(participante, '$.totalMinionsKilled') AS INTEGER)
      + CAST(json_extract_scalar(participante, '$.neutralMinionsKilled') AS INTEGER) AS cs,
    CAST(json_extract_scalar(participante, '$.goldEarned') AS INTEGER) AS oro,
    CAST(json_extract_scalar(participante, '$.totalDamageDealtToChampions') AS INTEGER) AS dano_a_campeones,
    CAST(json_extract_scalar(participante, '$.visionScore') AS INTEGER) AS vision_score,
    CAST(json_extract_scalar(participante, '$.challenges.kda') AS DOUBLE) AS kda,
    CAST(json_extract_scalar(participante, '$.challenges.killParticipation') AS DOUBLE) AS participacion_kills,
    CAST(json_extract_scalar(participante, '$.challenges.teamDamagePercentage') AS DOUBLE) AS pct_dano_equipo,
    CAST(json_extract_scalar(participante, '$.challenges.damageTakenOnTeamPercentage') AS DOUBLE) AS pct_dano_recibido_equipo,
    CAST(json_extract_scalar(participante, '$.challenges.damagePerMinute') AS DOUBLE) AS dano_por_min,
    CAST(json_extract_scalar(participante, '$.challenges.goldPerMinute') AS DOUBLE) AS oro_por_min,
    CAST(json_extract_scalar(participante, '$.challenges.visionScorePerMinute') AS DOUBLE) AS vision_por_min,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.laneMinionsFirst10Minutes') AS DOUBLE) AS INTEGER) AS cs_primeros_10,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.jungleCsBefore10Minutes') AS DOUBLE) AS INTEGER) AS jungla_cs_antes_10,
    CAST(json_extract_scalar(participante, '$.challenges.maxCsAdvantageOnLaneOpponent') AS DOUBLE) AS ventaja_cs_rival,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.maxLevelLeadLaneOpponent') AS DOUBLE) AS INTEGER) AS ventaja_nivel_rival,
    CAST(json_extract_scalar(participante, '$.challenges.visionScoreAdvantageLaneOpponent') AS DOUBLE) AS ventaja_vision_rival,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.laningPhaseGoldExpAdvantage') AS DOUBLE) AS INTEGER) AS ventaja_oro_xp_lineas,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.earlyLaningPhaseGoldExpAdvantage') AS DOUBLE) AS INTEGER) AS ventaja_oro_xp_temprana,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.turretPlatesTaken') AS DOUBLE) AS INTEGER) AS placas_torre,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.takedownsFirstXMinutes') AS DOUBLE) AS INTEGER) AS takedowns_primeros_min,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.takedowns') AS DOUBLE) AS INTEGER) AS takedowns,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.soloKills') AS DOUBLE) AS INTEGER) AS solo_kills,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.outnumberedKills') AS DOUBLE) AS INTEGER) AS kills_en_inferioridad,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.skillshotsDodged') AS DOUBLE) AS INTEGER) AS skillshots_esquivados,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.skillshotsHit') AS DOUBLE) AS INTEGER) AS skillshots_acertados,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.enemyChampionImmobilizations') AS DOUBLE) AS INTEGER) AS inmovilizaciones,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.killsNearEnemyTurret') AS DOUBLE) AS INTEGER) AS kills_cerca_torre_enemiga,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.killsUnderOwnTurret') AS DOUBLE) AS INTEGER) AS kills_bajo_torre_propia,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.multikills') AS DOUBLE) AS INTEGER) AS multikills,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.deathsByEnemyChamps') AS DOUBLE) AS INTEGER) AS muertes_por_campeones,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.maxKillDeficit') AS DOUBLE) AS INTEGER) AS deficit_kills_max,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.survivedSingleDigitHpCount') AS DOUBLE) AS INTEGER) AS sobrevivio_hp_baja,
    CAST(CAST(json_extract_scalar(participante, '$.wardsPlaced') AS DOUBLE) AS INTEGER) AS wards_puestas,
    CAST(CAST(json_extract_scalar(participante, '$.wardsKilled') AS DOUBLE) AS INTEGER) AS wards_destruidas,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.controlWardsPlaced') AS DOUBLE) AS INTEGER) AS wards_control,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.stealthWardsPlaced') AS DOUBLE) AS INTEGER) AS wards_sigilo,
    CAST(CAST(json_extract_scalar(participante, '$.detectorWardsPlaced') AS DOUBLE) AS INTEGER) AS wards_detectoras,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.wardTakedowns') AS DOUBLE) AS INTEGER) AS wards_takedowns,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.wardTakedownsBefore20M') AS DOUBLE) AS INTEGER) AS wards_takedowns_antes_20,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.wardsGuarded') AS DOUBLE) AS INTEGER) AS wards_protegidas,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.dragonTakedowns') AS DOUBLE) AS INTEGER) AS dragones_takedowns,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.baronTakedowns') AS DOUBLE) AS INTEGER) AS barones_takedowns,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.riftHeraldTakedowns') AS DOUBLE) AS INTEGER) AS heraldos_takedowns,
    CAST(CAST(json_extract_scalar(participante, '$.turretTakedowns') AS DOUBLE) AS INTEGER) AS torres_takedowns,
    CAST(CAST(json_extract_scalar(participante, '$.turretKills') AS DOUBLE) AS INTEGER) AS torres_destruidas,
    CAST(CAST(json_extract_scalar(participante, '$.inhibitorTakedowns') AS DOUBLE) AS INTEGER) AS inhibidores_takedowns,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.scuttleCrabKills') AS DOUBLE) AS INTEGER) AS cangrejos,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.epicMonsterSteals') AS DOUBLE) AS INTEGER) AS robos_epicos,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.alliedJungleMonsterKills') AS DOUBLE) AS INTEGER) AS jungla_aliada,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.enemyJungleMonsterKills') AS DOUBLE) AS INTEGER) AS jungla_enemiga,
    CAST(CAST(json_extract_scalar(participante, '$.damageDealtToObjectives') AS DOUBLE) AS INTEGER) AS dano_a_objetivos,
    CAST(CAST(json_extract_scalar(participante, '$.damageDealtToTurrets') AS DOUBLE) AS INTEGER) AS dano_a_torres,
    CAST(json_extract_scalar(participante, '$.firstBloodKill') AS BOOLEAN) AS primera_sangre,
    CAST(json_extract_scalar(participante, '$.firstTowerKill') AS BOOLEAN) AS primera_torre,
    CAST(CAST(json_extract_scalar(participante, '$.totalTimeSpentDead') AS DOUBLE) AS INTEGER) AS tiempo_muerto_seg,
    CAST(CAST(json_extract_scalar(participante, '$.longestTimeSpentLiving') AS DOUBLE) AS INTEGER) AS mayor_tiempo_vivo_seg,
    CAST(CAST(json_extract_scalar(participante, '$.damageSelfMitigated') AS DOUBLE) AS INTEGER) AS dano_mitigado,
    CAST(CAST(json_extract_scalar(participante, '$.totalDamageTaken') AS DOUBLE) AS INTEGER) AS dano_recibido,
    CAST(CAST(json_extract_scalar(participante, '$.totalHeal') AS DOUBLE) AS INTEGER) AS curacion,
    CAST(CAST(json_extract_scalar(participante, '$.totalHealsOnTeammates') AS DOUBLE) AS INTEGER) AS curacion_aliados,
    CAST(CAST(json_extract_scalar(participante, '$.totalDamageShieldedOnTeammates') AS DOUBLE) AS INTEGER) AS escudos_aliados,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.effectiveHealAndShielding') AS DOUBLE) AS INTEGER) AS sanacion_efectiva,
    CAST(json_extract_scalar(participante, '$.timeCCingOthers') AS DOUBLE) AS tiempo_cc_seg,
    CAST(CAST(json_extract_scalar(participante, '$.champLevel') AS DOUBLE) AS INTEGER) AS nivel,
    CAST(CAST(json_extract_scalar(participante, '$.goldSpent') AS DOUBLE) AS INTEGER) AS oro_gastado,
    CAST(CAST(json_extract_scalar(participante, '$.itemsPurchased') AS DOUBLE) AS INTEGER) AS items_comprados,
    CAST(CAST(json_extract_scalar(participante, '$.summoner1Id') AS DOUBLE) AS INTEGER) AS hechizo1,
    CAST(CAST(json_extract_scalar(participante, '$.summoner2Id') AS DOUBLE) AS INTEGER) AS hechizo2,
    json_extract_scalar(participante, '$.individualPosition') AS posicion_individual,
    CAST(json_extract_scalar(participante, '$.gameEndedInSurrender') AS BOOLEAN) AS rendicion,
    parche AS parche,
    puuid
FROM nuevos
"""


# Baseline de pares: los otros 9 jugadores de cada partida.
#
# El matchmaking los empareja al mismo MMR que el jugador rastreado, así
# que son una referencia de su propio elo que ya está guardada en la capa
# raw. Sin esto, el coaching solo puede decir "cambiaste", nunca "esto
# está por debajo del nivel al que juegas".
#
# Se guardan los 10 participantes, no 9: incluir al jugador rastreado
# hace la tabla autosuficiente, y las consultas lo excluyen por puuid.
# Se descartan las filas sin posición asignada, porque toda comparación
# de pares es dentro del mismo rol.
SQL_PARES_TEMPLATE = """
INSERT INTO peers_curated
WITH nuevos AS (
    SELECT
        r.match_id,
        r.game_creation,
        r.game_duration,
        r.queue_id,
        p.participante
    FROM matches_raw r
    CROSS JOIN UNNEST(
        CAST(json_extract(r.payload, '$.info.participants') AS ARRAY(JSON))
    ) AS p(participante)
    WHERE r.year || r.month || r.day >=
          date_format(date_add('day', -{lookback}, current_date), '%Y%m%d')
      AND json_extract_scalar(p.participante, '$.teamPosition') <> ''
      AND NOT EXISTS (
          SELECT 1
          FROM peers_curated c
          WHERE c.match_id = r.match_id
            AND c.puuid = json_extract_scalar(p.participante, '$.puuid')
      )
)
SELECT
    match_id,
    json_extract_scalar(participante, '$.puuid') AS puuid,
    from_unixtime(game_creation / 1000) AS jugada_en,
    CAST(queue_id AS INTEGER) AS queue_id,
    round(game_duration / 60.0, 1) AS duracion_min,
    json_extract_scalar(participante, '$.championName') AS campeon,
    CAST(json_extract_scalar(participante, '$.win') AS BOOLEAN) AS victoria,
    CAST(CAST(json_extract_scalar(participante, '$.kills')   AS DOUBLE) AS INTEGER) AS kills,
    CAST(CAST(json_extract_scalar(participante, '$.deaths')  AS DOUBLE) AS INTEGER) AS deaths,
    CAST(CAST(json_extract_scalar(participante, '$.assists') AS DOUBLE) AS INTEGER) AS assists,
    CAST(json_extract_scalar(participante, '$.challenges.kda') AS DOUBLE) AS kda,
    CAST(json_extract_scalar(participante, '$.challenges.killParticipation') AS DOUBLE) AS participacion_kills,
    CAST(json_extract_scalar(participante, '$.challenges.teamDamagePercentage') AS DOUBLE) AS pct_dano_equipo,
    (CAST(json_extract_scalar(participante, '$.totalMinionsKilled') AS DOUBLE)
      + CAST(json_extract_scalar(participante, '$.neutralMinionsKilled') AS DOUBLE))
      / nullif(game_duration / 60.0, 0) AS cs_por_min,
    CAST(json_extract_scalar(participante, '$.challenges.damagePerMinute') AS DOUBLE) AS dano_por_min,
    CAST(json_extract_scalar(participante, '$.challenges.goldPerMinute') AS DOUBLE) AS oro_por_min,
    CAST(json_extract_scalar(participante, '$.challenges.visionScorePerMinute') AS DOUBLE) AS vision_por_min,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.laneMinionsFirst10Minutes') AS DOUBLE) AS INTEGER) AS cs_primeros_10,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.jungleCsBefore10Minutes') AS DOUBLE) AS INTEGER) AS jungla_cs_antes_10,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.turretPlatesTaken') AS DOUBLE) AS INTEGER) AS placas_torre,
    CAST(json_extract_scalar(participante, '$.challenges.maxCsAdvantageOnLaneOpponent') AS DOUBLE) AS ventaja_cs_rival,
    CAST(json_extract_scalar(participante, '$.challenges.visionScoreAdvantageLaneOpponent') AS DOUBLE) AS ventaja_vision_rival,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.controlWardsPlaced') AS DOUBLE) AS INTEGER) AS wards_control,
    CAST(CAST(json_extract_scalar(participante, '$.wardsKilled') AS DOUBLE) AS INTEGER) AS wards_destruidas,
    CAST(CAST(json_extract_scalar(participante, '$.totalTimeSpentDead') AS DOUBLE) AS INTEGER) AS tiempo_muerto_seg,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.soloKills') AS DOUBLE) AS INTEGER) AS solo_kills,
    CAST(CAST(json_extract_scalar(participante, '$.challenges.deathsByEnemyChamps') AS DOUBLE) AS INTEGER) AS muertes_por_campeones,
    json_extract_scalar(participante, '$.teamPosition') AS rol
FROM nuevos
"""


# Proyección del timeline: una fila por minuto, con el rival de línea al
# lado.
#
# El rival se resuelve por (misma partida, mismo rol, distinto puuid)
# usando peers_curated, que ya tiene el rol de los 10 jugadores. El
# min() garantiza un único rival por partida aunque el emparejamiento de
# roles venga raro.
#
# El mapa participantId -> puuid viene dentro del propio timeline, y se
# resuelve con un CAST a MAP porque la clave del frame es dinámica y
# json_extract exige una ruta constante.
SQL_TIMELINE_TEMPLATE = """
INSERT INTO timeline_frames
WITH ids AS (
    SELECT
        t.match_id,
        json_extract_scalar(p, '$.puuid') AS puuid,
        json_extract_scalar(p, '$.participantId') AS pid
    FROM timelines_raw t
    CROSS JOIN UNNEST(
        CAST(json_extract(t.payload, '$.info.participants') AS ARRAY(JSON))
    ) AS x(p)
    WHERE t.year || t.month || t.day >=
          date_format(date_add('day', -{lookback}, current_date), '%Y%m%d')
),
-- Además de los frames por minuto, el timeline trae uno final en el
-- instante en que termina la partida. Ese último cae en el mismo minuto
-- que el frame regular anterior, así que sin deduplicar cada partida
-- aportaría un minuto repetido. Se conserva el primero de cada minuto,
-- que es el que cae sobre el límite exacto.
frames AS (
    SELECT match_id, minuto, pf
    FROM (
        SELECT
            t.match_id,
            CAST(CAST(json_extract_scalar(f, '$.timestamp') AS BIGINT) / 60000 AS INTEGER) AS minuto,
            CAST(json_extract(f, '$.participantFrames') AS MAP(VARCHAR, JSON)) AS pf,
            row_number() OVER (
                PARTITION BY
                    t.match_id,
                    CAST(CAST(json_extract_scalar(f, '$.timestamp') AS BIGINT) / 60000 AS INTEGER)
                ORDER BY CAST(json_extract_scalar(f, '$.timestamp') AS BIGINT)
            ) AS orden
        FROM timelines_raw t
        CROSS JOIN UNNEST(
            CAST(json_extract(t.payload, '$.info.frames') AS ARRAY(JSON))
        ) AS y(f)
        WHERE t.year || t.month || t.day >=
              date_format(date_add('day', -{lookback}, current_date), '%Y%m%d')
    )
    WHERE orden = 1
),
jugador AS (
    SELECT m.match_id, m.puuid, m.rol, m.campeon, m.victoria
    FROM matches_curated m
    WHERE m.rol <> ''
      AND NOT EXISTS (
          SELECT 1 FROM timeline_frames tf
          WHERE tf.match_id = m.match_id AND tf.puuid = m.puuid
      )
),
rival AS (
    SELECT j.match_id, j.puuid AS puuid_jugador, min(p.puuid) AS puuid_rival
    FROM jugador j
    JOIN peers_curated p
      ON p.match_id = j.match_id AND p.rol = j.rol AND p.puuid <> j.puuid
    GROUP BY j.match_id, j.puuid
)
SELECT
    f.match_id,
    f.minuto,
    j.rol,
    j.campeon,
    j.victoria,
    CAST(json_extract_scalar(f.pf[iy.pid], '$.totalGold') AS INTEGER) AS oro,
    CAST(json_extract_scalar(f.pf[iy.pid], '$.xp') AS INTEGER) AS xp,
    CAST(json_extract_scalar(f.pf[iy.pid], '$.minionsKilled') AS INTEGER)
      + CAST(json_extract_scalar(f.pf[iy.pid], '$.jungleMinionsKilled') AS INTEGER) AS cs,
    CAST(json_extract_scalar(f.pf[iy.pid], '$.level') AS INTEGER) AS nivel,
    CAST(json_extract_scalar(f.pf[ir.pid], '$.totalGold') AS INTEGER) AS oro_rival,
    CAST(json_extract_scalar(f.pf[ir.pid], '$.xp') AS INTEGER) AS xp_rival,
    CAST(json_extract_scalar(f.pf[ir.pid], '$.minionsKilled') AS INTEGER)
      + CAST(json_extract_scalar(f.pf[ir.pid], '$.jungleMinionsKilled') AS INTEGER) AS cs_rival,
    CAST(json_extract_scalar(f.pf[ir.pid], '$.level') AS INTEGER) AS nivel_rival,
    CAST(json_extract_scalar(f.pf[iy.pid], '$.totalGold') AS INTEGER)
      - CAST(json_extract_scalar(f.pf[ir.pid], '$.totalGold') AS INTEGER) AS diff_oro,
    CAST(json_extract_scalar(f.pf[iy.pid], '$.xp') AS INTEGER)
      - CAST(json_extract_scalar(f.pf[ir.pid], '$.xp') AS INTEGER) AS diff_xp,
    (CAST(json_extract_scalar(f.pf[iy.pid], '$.minionsKilled') AS INTEGER)
      + CAST(json_extract_scalar(f.pf[iy.pid], '$.jungleMinionsKilled') AS INTEGER))
      - (CAST(json_extract_scalar(f.pf[ir.pid], '$.minionsKilled') AS INTEGER)
      + CAST(json_extract_scalar(f.pf[ir.pid], '$.jungleMinionsKilled') AS INTEGER)) AS diff_cs,
    j.puuid
FROM frames f
JOIN jugador j ON j.match_id = f.match_id
JOIN ids iy ON iy.match_id = f.match_id AND iy.puuid = j.puuid
LEFT JOIN rival r ON r.match_id = f.match_id AND r.puuid_jugador = j.puuid
LEFT JOIN ids ir ON ir.match_id = f.match_id AND ir.puuid = r.puuid_rival
"""


def validar_lookback(event) -> int:
    """Valida la ventana normal o histórica solicitada."""
    raw_lookback = (event or {}).get("lookback_days", LOOKBACK_DAYS)
    if isinstance(raw_lookback, bool):
        raise ValueError("lookback_days debe ser un entero")
    try:
        lookback = int(raw_lookback)
    except (TypeError, ValueError) as error:
        raise ValueError("lookback_days debe ser un entero") from error
    if not 1 <= lookback <= 3650:
        raise ValueError("lookback_days debe estar entre 1 y 3650")
    return lookback


def adquirir_bloqueo(context) -> str:
    tabla = dynamodb.Table(WATERMARK_TABLE)
    ahora = int(time.time())
    owner = getattr(context, "aws_request_id", None) or f"local-{time.time_ns()}"
    try:
        tabla.put_item(
            Item={
                "puuid": LOCK_KEY,
                "owner": owner,
                "expires_at": ahora + LOCK_TTL_SECONDS,
            },
            ConditionExpression="attribute_not_exists(puuid) OR expires_at < :now",
            ExpressionAttributeValues={":now": ahora},
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise RuntimeError("Ya hay otra ejecución de curado en curso") from error
        raise
    return owner


def liberar_bloqueo(owner: str) -> None:
    tabla = dynamodb.Table(WATERMARK_TABLE)
    try:
        tabla.delete_item(
            Key={"puuid": LOCK_KEY},
            ConditionExpression="#owner = :owner",
            ExpressionAttributeNames={"#owner": "owner"},
            ExpressionAttributeValues={":owner": owner},
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        logger.warning("El lock de curado ya había expirado o cambiado de dueño")


def ejecutar_consulta(nombre: str, sql: str, context) -> dict:
    """Lanza una consulta en Athena y espera a que termine."""
    qid = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
        QueryExecutionContext={"Database": DATABASE},
    )["QueryExecutionId"]

    while True:
        ejecucion = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        estado = ejecucion["Status"]["State"]
        if estado in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if context and context.get_remaining_time_in_millis() <= 15_000:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"{nombre} cancelado antes del timeout de Lambda")
        time.sleep(2)

    if estado != "SUCCEEDED":
        razon = ejecucion["Status"].get("StateChangeReason", "sin detalle")
        raise RuntimeError(f"{nombre} {estado}: {razon}")

    stats = ejecucion.get("Statistics", {})
    escaneado_mb = stats.get("DataScannedInBytes", 0) / 1_000_000
    logger.info(
        "%s OK: %.1f MB escaneados en %s ms",
        nombre, escaneado_mb, stats.get("TotalExecutionTimeInMillis", "?"),
    )
    return {"estado": estado, "escaneado_mb": round(escaneado_mb, 2)}


def ejecutar_curado(lookback: int, context) -> dict:
    """
    Cura las partidas del jugador y, aparte, el baseline de pares.

    Van en dos consultas y no en una porque escriben tablas distintas:
    matches_curated guarda solo al jugador rastreado con el detalle
    completo, y peers_curated guarda a los 10 con las métricas mínimas
    para comparar.
    """
    jugador = ejecutar_consulta(
        "Curado", SQL_TEMPLATE.format(lookback=lookback), context
    )
    pares = ejecutar_consulta(
        "Baseline de pares", SQL_PARES_TEMPLATE.format(lookback=lookback), context
    )
    # El timeline va al final porque depende de las otras dos: necesita
    # el rol del jugador (matches_curated) y el de su rival
    # (peers_curated) para poder emparejarlos.
    timeline = ejecutar_consulta(
        "Timeline", SQL_TIMELINE_TEMPLATE.format(lookback=lookback), context
    )
    return {
        "estado": "SUCCEEDED",
        "escaneado_mb": round(
            jugador["escaneado_mb"] + pares["escaneado_mb"] + timeline["escaneado_mb"], 2
        ),
        "jugador": jugador,
        "pares": pares,
        "timeline": timeline,
    }


def lambda_handler(event, context):
    # Para backfills se puede invocar a mano con {"lookback_days": N} y
    # cubrir más historia que la ventana normal de 3 días.
    lookback = validar_lookback(event)
    lock_owner = adquirir_bloqueo(context)
    try:
        return ejecutar_curado(lookback, context)
    finally:
        liberar_bloqueo(lock_owner)
