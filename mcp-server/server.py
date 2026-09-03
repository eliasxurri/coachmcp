"""
Servidor MCP que expone el data lake de partidas como herramientas.

Corre local por stdio y consulta Athena con boto3. No toca la API de
Riot: lee solo lo que la Lambda ya ingirió, así que funciona aunque la
development key esté expirada.

Las consultas SQL son las mismas de queries.sql, parametrizadas. Todas
filtran por la partición puuid (y por fecha cuando aplica) para que
cada consulta escanee KBs, no el bucket completo.
"""

import os
import re
import time
from datetime import datetime, timedelta, timezone

import boto3
from mcp.server.mcpserver import MCPServer

import estadistica

AWS_REGION = os.environ.get("AWS_REGION", "sa-east-1")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "lol-pipeline")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "lol_pipeline_db")

# Nombres legibles para los queue IDs que aparecen en el caso de uso.
QUEUES = {
    420: "ranked solo/duo",
    440: "ranked flex",
    450: "ARAM",
    400: "normal draft",
    430: "normal blind",
    900: "URF",
    1700: "Arena",
    1710: "Arena",
    1750: "Arena",
}

athena = boto3.client("athena", region_name=AWS_REGION)

mcp = MCPServer("lol-coach")


def run_query(sql: str, timeout_s: int = 60) -> list[dict]:
    """Ejecuta una consulta en Athena y devuelve filas como dicts."""
    qid = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
    )["QueryExecutionId"]

    deadline = time.monotonic() + timeout_s
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Consulta Athena {state}: {status.get('StateChangeReason', '?')}")
        if time.monotonic() > deadline:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Consulta Athena excedió {timeout_s}s")
        time.sleep(0.5)

    rows: list[dict] = []
    columns: list[str] = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=qid):
        data = page["ResultSet"]["Rows"]
        if not columns:
            columns = [c["VarCharValue"] for c in data[0]["Data"]]
            data = data[1:]
        for row in data:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            rows.append(dict(zip(columns, values)))
    return rows


def coercer(rows: list[dict]) -> list[dict]:
    """Athena devuelve todo como texto; convierte lo que parezca número."""
    for row in rows:
        for key, value in row.items():
            if not isinstance(value, str):
                continue
            if re.fullmatch(r"-?\d+", value):
                row[key] = int(value)
            elif re.fullmatch(r"-?\d+\.\d+", value):
                row[key] = float(value)
    return rows


def sql_str(value: str) -> str:
    """Escapa un literal de texto para interpolar en SQL."""
    return "'" + value.replace("'", "''") + "'"


def resolver_puuid(player: str | None) -> str:
    """
    Convierte un Riot ID ("nombre#tag"), un prefijo de nombre o None en
    un PUUID, usando los datos ya ingeridos (no la API de Riot).

    Con None: si hay un solo jugador en el lake, se usa ese.
    """
    jugadores = listar_jugadores()
    if not jugadores:
        raise ValueError("El data lake está vacío: aún no se ingirió ninguna partida.")

    if player is None:
        if len(jugadores) == 1:
            return jugadores[0]["puuid"]
        nombres = ", ".join(j["riot_id"] for j in jugadores)
        raise ValueError(f"Hay varios jugadores; indica cuál: {nombres}")

    if re.fullmatch(r"[A-Za-z0-9_-]{70,90}", player):
        return player  # ya es un PUUID

    buscado = player.lower()
    for j in jugadores:
        if j["riot_id"].lower() == buscado or j["game_name"].lower() == buscado:
            return j["puuid"]
    nombres = ", ".join(j["riot_id"] for j in jugadores)
    raise ValueError(f"Jugador '{player}' no encontrado. Disponibles: {nombres}")


def listar_jugadores() -> list[dict]:
    """Jugadores presentes en la capa curada, con su Riot ID."""
    rows = run_query("""
        SELECT
            puuid,
            max(game_name) AS game_name,
            max(tag_line)  AS tag_line,
            count(*) AS partidas
        FROM matches_curated
        GROUP BY puuid
    """)
    return [
        {
            "puuid": r["puuid"],
            "game_name": r["game_name"] or "?",
            "riot_id": f"{r['game_name'] or '?'}#{r['tag_line'] or '?'}",
            "partidas_ingeridas": int(r["partidas"]),
        }
        for r in rows
    ]


def filtro_fecha(days: int, alias: str = "") -> str:
    """
    Predicado sobre jugada_en para los últimos N días.

    `alias` califica la columna cuando la consulta une varias tablas
    (por ejemplo "m" para m.jugada_en).
    """
    desde = datetime.now(timezone.utc) - timedelta(days=days)
    columna = f"{alias}.jugada_en" if alias else "jugada_en"
    return f"{columna} >= timestamp {sql_str(desde.strftime('%Y-%m-%d %H:%M:%S'))}"


@mcp.tool()
def list_players() -> list[dict]:
    """Lista los jugadores con partidas en el data lake y cuántas tiene cada uno."""
    return listar_jugadores()


@mcp.tool()
def get_recent_matches(
    player: str | None = None,
    days: int = 7,
    limit: int = 20,
    solo_only: bool = True,
) -> list[dict]:
    """
    Partidas recientes de un jugador con sus estadísticas: campeón, rol,
    resultado, KDA, CS, daño y duración.

    Args:
        player: Riot ID ("nombre#tag"), nombre a secas, o vacío si solo
            hay un jugador rastreado.
        days: ventana hacia atrás en días (por fecha de juego).
        limit: máximo de partidas a devolver, las más recientes primero.
        solo_only: por defecto True, solo ranked solo/duo (cola 420).
            Flex queda fuera a propósito: es un modo menos serio y
            mezclarlo ensucia cualquier conclusión.
    """
    puuid = resolver_puuid(player)
    filtro_cola = "AND queue_id = 420" if solo_only else ""
    rows = run_query(f"""
        SELECT
            match_id,
            CAST(jugada_en AS VARCHAR) AS jugada_en,
            queue_id,
            campeon,
            rol,
            CAST(victoria AS VARCHAR) AS victoria,
            kills, deaths, assists,
            round(kda, 2) AS kda,
            round(participacion_kills, 3) AS participacion_kills,
            round(pct_dano_equipo, 3) AS pct_dano_equipo,
            cs, oro, dano_a_campeones, vision_score,
            round(dano_por_min, 1) AS dano_por_min,
            round(oro_por_min, 1) AS oro_por_min,
            cs_primeros_10, placas_torre, solo_kills,
            wards_control, wards_destruidas,
            tiempo_muerto_seg,
            duracion_min,
            parche
        FROM matches_curated
        WHERE puuid = {sql_str(puuid)}
          AND {filtro_fecha(days)}
          {filtro_cola}
        ORDER BY jugada_en DESC
        LIMIT {int(limit)}
    """)
    for r in rows:
        qid = int(r["queue_id"])
        r["cola"] = QUEUES.get(qid, f"queue {qid}")
        r["victoria"] = r["victoria"] == "true"
    return coercer(rows)


@mcp.tool()
def get_champion_stats(
    player: str | None = None,
    days: int = 90,
    solo_only: bool = True,
    min_games: int = 1,
) -> list[dict]:
    """
    Winrate, KDA y promedios por campeón para un jugador.

    Args:
        player: Riot ID ("nombre#tag"), nombre a secas, o vacío si solo
            hay un jugador rastreado.
        days: ventana hacia atrás en días.
        solo_only: por defecto True, solo ranked solo/duo (cola 420).
            Flex queda fuera a propósito: es un modo menos serio y
            mezclarlo ensucia cualquier conclusión.
        min_games: descarta campeones con menos partidas que esto.
    """
    puuid = resolver_puuid(player)
    filtro_cola = "AND queue_id = 420" if solo_only else ""
    rows = run_query(f"""
        SELECT
            campeon,
            count(*) AS partidas,
            round(100.0 * sum(CASE WHEN victoria THEN 1 ELSE 0 END) / count(*), 1) AS winrate_pct,
            round(avg(CAST(kills   AS DOUBLE)), 1) AS kills_prom,
            round(avg(CAST(deaths  AS DOUBLE)), 1) AS deaths_prom,
            round(avg(CAST(assists AS DOUBLE)), 1) AS assists_prom,
            round(avg(kda), 2) AS kda,
            round(avg(participacion_kills), 3) AS participacion_kills,
            round(avg(pct_dano_equipo), 3) AS pct_dano_equipo,
            round(avg(CAST(cs AS DOUBLE) / nullif(duracion_min, 0)), 1) AS cs_por_min,
            round(avg(CAST(cs_primeros_10 AS DOUBLE)), 1) AS cs_primeros_10,
            round(avg(dano_por_min), 1) AS dano_por_min,
            round(avg(oro_por_min), 1) AS oro_por_min,
            round(avg(vision_por_min), 2) AS vision_por_min,
            round(avg(CAST(vision_score AS DOUBLE)), 1) AS vision_prom,
            round(avg(CAST(tiempo_muerto_seg AS DOUBLE)), 0) AS tiempo_muerto_seg
        FROM matches_curated
        WHERE puuid = {sql_str(puuid)}
          AND {filtro_fecha(days)}
          {filtro_cola}
        GROUP BY campeon
        HAVING count(*) >= {int(min_games)}
        ORDER BY partidas DESC
    """)
    return coercer(rows)


# Métricas que se comparan entre ventanas: (columna, sentido, expresión).
#
# Daño, oro y visión por minuto los calcula Riot en `challenges` y se
# guardan tal cual en la capa curada; solo CS por minuto se deriva,
# porque Riot no lo publica. Normalizar importa: si la duración media
# cambia entre ventanas, los totales miden cuánto duró la partida tanto
# como cuán bien se jugó.
#
# `duracion_min` va como neutra a propósito: partidas más largas no son
# mejores ni peores por sí solas (pueden ser remontadas o estancamientos),
# pero el dato ayuda al asistente a interpretar el resto.
#
# Las tres métricas "contra el rival de línea" cubren el 95-100% de las
# partidas de Grieta en todos los roles, jungla incluida. Faltan solo en
# modos sin líneas (Arena, ARAM, URF) y en remakes. Aun así cada métrica
# lleva su propio conteo: el filtro de cola las deja casi completas, pero
# usar el total de la ventana igual inflaría la muestra.
METRICAS = [
    # Resultado y combate
    ("kda", "mayor", None),
    ("participacion_kills", "mayor", None),
    ("kills", "mayor", None),
    ("deaths", "menor", None),
    ("assists", "mayor", None),
    ("muertes_por_campeones", "menor", None),
    ("solo_kills", "mayor", None),
    # Economía y daño
    ("cs_por_min", "mayor", "CAST(cs AS DOUBLE) / nullif(duracion_min, 0)"),
    ("oro_por_min", "mayor", None),
    ("dano_por_min", "mayor", None),
    ("pct_dano_equipo", "mayor", None),
    # Fase de líneas
    # cs_primeros_10 cuenta súbditos de línea y da casi cero en jungla;
    # jungla_cs_antes_10 es su equivalente para ese rol. Se comparan las
    # dos y la que no aplique al rol quedará plana.
    ("cs_primeros_10", "mayor", None),
    ("jungla_cs_antes_10", "mayor", None),
    ("placas_torre", "mayor", None),
    ("ventaja_cs_rival", "mayor", None),
    ("ventaja_nivel_rival", "mayor", None),
    ("ventaja_vision_rival", "mayor", None),
    # Visión
    ("vision_por_min", "mayor", None),
    ("wards_control", "mayor", None),
    ("wards_destruidas", "mayor", None),
    # Tempo
    ("tiempo_muerto_seg", "menor", None),
    ("duracion_min", "neutro", None),
]


def _direccion(delta: float, sentido: str) -> str:
    """Traduce un delta a mejora/empeora según el sentido de la métrica."""
    if sentido == "neutro" or delta == 0:
        return "sin cambio" if delta == 0 else "neutro"
    subio = delta > 0
    mejor_si_sube = sentido == "mayor"
    return "mejora" if subio == mejor_si_sube else "empeora"


def _redondear(valor, decimales=2):
    return None if valor is None else round(float(valor), decimales)


@mcp.tool()
def get_trends(
    player: str | None = None,
    days: int = 30,
    solo_only: bool = True,
) -> dict:
    """
    Compara la ventana reciente contra la anterior de igual duración y
    dice qué cambió de forma estadísticamente significativa.

    Cada métrica trae su p-valor y una `clasificacion` de tres niveles:

    - "significativo": sobrevive la corrección por comparaciones
      múltiples. Se puede afirmar como cambio real.
    - "indicio": pasa el umbral simple pero no la corrección. Vale como
      pista a vigilar, nunca como conclusión.
    - "ruido": indistinguible del azar. NO debe reportarse como mejora ni
      como bajón, por grande que parezca el delta.

    `cobertura_parcial` marca las métricas que Riot no publica en todas
    las partidas (las de ventaja sobre el rival de línea faltan en jungla).

    Args:
        player: Riot ID ("nombre#tag"), nombre a secas, o vacío si solo
            hay un jugador rastreado.
        days: duración de cada ventana. days=30 compara los últimos 30
            días contra los 30 anteriores.
        solo_only: por defecto True, solo ranked solo/duo (cola 420).
            Flex queda fuera a propósito: es un modo menos serio y
            mezclarlo ensucia cualquier conclusión.
    """
    puuid = resolver_puuid(player)
    filtro_cola = "AND queue_id = 420" if solo_only else ""

    ahora = datetime.now(timezone.utc)
    corte = ahora - timedelta(days=days)
    inicio = ahora - timedelta(days=days * 2)
    fmt = "%Y-%m-%d %H:%M:%S"

    fuentes = ",\n                ".join(
        f"CAST({expresion or nombre} AS DOUBLE) AS {nombre}"
        for nombre, _, expresion in METRICAS
    )
    columnas = ",\n            ".join(
        f"avg({m}) AS {m}_avg, stddev_samp({m}) AS {m}_sd, count({m}) AS {m}_n"
        for m, _, _ in METRICAS
    )
    rows = run_query(f"""
        WITH ventanas AS (
            SELECT
                CASE WHEN jugada_en >= timestamp {sql_str(corte.strftime(fmt))}
                     THEN 'reciente' ELSE 'previo' END AS ventana,
                CASE WHEN victoria THEN 1 ELSE 0 END AS win,
                {fuentes}
            FROM matches_curated
            WHERE puuid = {sql_str(puuid)}
              AND jugada_en >= timestamp {sql_str(inicio.strftime(fmt))}
              {filtro_cola}
        )
        SELECT ventana, count(*) AS n, sum(win) AS victorias,
            {columnas}
        FROM ventanas
        GROUP BY ventana
    """)

    ventanas = {r["ventana"]: r for r in rows}
    reciente = ventanas.get("reciente", {})
    previo = ventanas.get("previo", {})
    n_rec = int(reciente.get("n") or 0)
    n_prev = int(previo.get("n") or 0)
    v_rec = int(reciente.get("victorias") or 0)
    v_prev = int(previo.get("victorias") or 0)

    metricas = []

    # El winrate se compara como proporción, no como media: es una
    # variable binaria por partida, no una escala continua.
    if n_rec and n_prev:
        wr_rec = 100.0 * v_rec / n_rec
        wr_prev = 100.0 * v_prev / n_prev
        p = estadistica.comparar_proporciones(v_rec, n_rec, v_prev, n_prev)
        delta = wr_rec - wr_prev
        metricas.append({
            "metrica": "winrate_pct",
            "reciente": round(wr_rec, 1),
            "previo": round(wr_prev, 1),
            "delta": round(delta, 1),
            "n_reciente": n_rec,
            "n_previo": n_prev,
            "p_valor": _redondear(p, 4),
            "direccion": _direccion(delta, "mayor"),
        })

    for nombre, sentido, _ in METRICAS:
        media_rec = reciente.get(f"{nombre}_avg")
        media_prev = previo.get(f"{nombre}_avg")
        if media_rec is None or media_prev is None:
            continue
        media_rec, media_prev = float(media_rec), float(media_prev)
        sd_rec = reciente.get(f"{nombre}_sd")
        sd_prev = previo.get(f"{nombre}_sd")

        # Conteo propio de la métrica, no el de la ventana: las columnas
        # que Riot no siempre publica tienen menos filas útiles.
        n_metrica_rec = int(reciente.get(f"{nombre}_n") or 0)
        n_metrica_prev = int(previo.get(f"{nombre}_n") or 0)

        p = estadistica.comparar_medias(
            media_rec, float(sd_rec) if sd_rec else None, n_metrica_rec,
            media_prev, float(sd_prev) if sd_prev else None, n_metrica_prev,
        )
        delta = media_rec - media_prev
        fila = {
            "metrica": nombre,
            "reciente": round(media_rec, 2),
            "previo": round(media_prev, 2),
            "delta": round(delta, 2),
            "delta_pct": round(100.0 * delta / media_prev, 1) if media_prev else None,
            "n_reciente": n_metrica_rec,
            "n_previo": n_metrica_prev,
            "p_valor": _redondear(p, 4),
            "direccion": _direccion(delta, sentido),
        }
        # Marca las métricas con menos cobertura que la ventana, para que
        # el asistente sepa que no se midieron sobre todas las partidas.
        if n_metrica_rec < n_rec or n_metrica_prev < n_prev:
            fila["cobertura_parcial"] = True
        metricas.append(fila)

    # Corrección por comparaciones múltiples sobre el conjunto completo,
    # winrate incluido: con ~22 pruebas a la vez, el umbral simple daría
    # en promedio una significativa por azar en cada consulta.
    banderas = estadistica.ajustar_fdr([m["p_valor"] for m in metricas])
    for fila, significativa in zip(metricas, banderas):
        fila["significativo"] = significativa
        # Tres niveles en vez de un binario: una métrica que pasa el
        # umbral simple pero no la corrección no es ruido cualquiera, es
        # un candidato a mirar. Colapsarla a "no significativo" perdería
        # señal útil; llamarla "significativa" la sobrevendería.
        p = fila["p_valor"]
        if significativa:
            fila["clasificacion"] = "significativo"
        elif p is not None and p < estadistica.ALFA:
            fila["clasificacion"] = "indicio"
        else:
            fila["clasificacion"] = "ruido"

    significativas = [m for m in metricas if m["significativo"]]
    indicios = [m for m in metricas if m["clasificacion"] == "indicio"]
    suficiente = estadistica.muestra_suficiente(n_rec, n_prev)

    if not n_rec or not n_prev:
        nota = ("Falta una de las dos ventanas: no hay con qué comparar. "
                "Probá una ventana más larga.")
    elif not suficiente:
        nota = (f"Muestra chica ({n_rec} y {n_prev} partidas). Aunque alguna "
                "métrica salga significativa, conviene tratarla como indicio "
                "y no como conclusión.")
    elif not significativas:
        nota = (f"Ninguna de las {len(metricas)} métricas cambió más de lo que "
                "explica el azar: el rendimiento se mantuvo estable.")
    else:
        nota = (f"{len(significativas)} de {len(metricas)} métricas cambiaron de "
                "forma significativa tras corregir por comparaciones múltiples")
        if indicios:
            nota += (f"; otras {len(indicios)} quedaron como indicio (pasan el "
                     "umbral simple pero no la corrección) y valen como pista, "
                     "no como conclusión")
        nota += "."

    return {
        "jugador": player or "(único rastreado)",
        "ventana_dias": days,
        "solo_only": solo_only,
        "reciente": {
            "desde": corte.strftime(fmt), "hasta": ahora.strftime(fmt),
            "partidas": n_rec, "victorias": v_rec,
        },
        "previo": {
            "desde": inicio.strftime(fmt), "hasta": corte.strftime(fmt),
            "partidas": n_prev, "victorias": v_prev,
        },
        "muestra_suficiente": suficiente,
        "metricas": metricas,
        "cambios_significativos": [m["metrica"] for m in significativas],
        "indicios": [m["metrica"] for m in indicios],
        "nota": nota,
    }


@mcp.tool()
def get_champion_trends(
    player: str | None = None,
    days: int = 30,
    solo_only: bool = True,
    min_games: int = 3,
) -> list[dict]:
    """
    Cómo cambió el pool de campeones entre la ventana reciente y la
    anterior: cuáles son nuevos, cuáles se dejaron de jugar y en cuáles
    cambió el rendimiento.

    Con pocas partidas por campeón el p-valor casi nunca será
    significativo; eso es correcto y hay que respetarlo al interpretar.

    Args:
        player: Riot ID ("nombre#tag"), nombre a secas, o vacío si solo
            hay un jugador rastreado.
        days: duración de cada ventana.
        solo_only: por defecto True, solo ranked solo/duo (cola 420).
            Flex queda fuera a propósito: es un modo menos serio y
            mezclarlo ensucia cualquier conclusión.
        min_games: mínimo de partidas (sumando ambas ventanas) para
            incluir un campeón.
    """
    puuid = resolver_puuid(player)
    filtro_cola = "AND queue_id = 420" if solo_only else ""

    ahora = datetime.now(timezone.utc)
    corte = ahora - timedelta(days=days)
    inicio = ahora - timedelta(days=days * 2)
    fmt = "%Y-%m-%d %H:%M:%S"

    rows = run_query(f"""
        SELECT
            campeon,
            sum(CASE WHEN reciente THEN 1 ELSE 0 END) AS n_reciente,
            sum(CASE WHEN reciente AND victoria THEN 1 ELSE 0 END) AS v_reciente,
            sum(CASE WHEN NOT reciente THEN 1 ELSE 0 END) AS n_previo,
            sum(CASE WHEN NOT reciente AND victoria THEN 1 ELSE 0 END) AS v_previo
        FROM (
            SELECT campeon, victoria,
                   jugada_en >= timestamp {sql_str(corte.strftime(fmt))} AS reciente
            FROM matches_curated
            WHERE puuid = {sql_str(puuid)}
              AND jugada_en >= timestamp {sql_str(inicio.strftime(fmt))}
              {filtro_cola}
        )
        GROUP BY campeon
        HAVING count(*) >= {int(min_games)}
        ORDER BY sum(CASE WHEN reciente THEN 1 ELSE 0 END) DESC, count(*) DESC
    """)

    resultado = []
    for r in rows:
        n_rec, n_prev = int(r["n_reciente"]), int(r["n_previo"])
        v_rec, v_prev = int(r["v_reciente"]), int(r["v_previo"])

        if n_prev == 0:
            estado = "nuevo"
        elif n_rec == 0:
            estado = "abandonado"
        else:
            estado = "constante"

        fila = {
            "campeon": r["campeon"],
            "estado": estado,
            "partidas_reciente": n_rec,
            "partidas_previo": n_prev,
            "winrate_reciente": round(100.0 * v_rec / n_rec, 1) if n_rec else None,
            "winrate_previo": round(100.0 * v_prev / n_prev, 1) if n_prev else None,
        }

        if n_rec and n_prev:
            p = estadistica.comparar_proporciones(v_rec, n_rec, v_prev, n_prev)
            fila["delta_winrate"] = round(
                fila["winrate_reciente"] - fila["winrate_previo"], 1
            )
            fila["p_valor"] = _redondear(p, 4)
            fila["significativo"] = p is not None and p < estadistica.ALFA
        else:
            fila["delta_winrate"] = None
            fila["p_valor"] = None
            fila["significativo"] = False

        resultado.append(fila)

    return resultado


# Métricas que se comparan contra los pares del mismo rol.
# (columna, sentido, expresión en matches_curated, expresión en peers_curated)
METRICAS_PARES = [
    ("kda", "mayor", None, None),
    ("participacion_kills", "mayor", None, None),
    ("pct_dano_equipo", "mayor", None, None),
    ("cs_por_min", "mayor", "CAST(cs AS DOUBLE) / nullif(duracion_min, 0)", None),
    ("oro_por_min", "mayor", None, None),
    ("dano_por_min", "mayor", None, None),
    ("vision_por_min", "mayor", None, None),
    ("cs_primeros_10", "mayor", None, None),
    ("jungla_cs_antes_10", "mayor", None, None),
    ("placas_torre", "mayor", None, None),
    ("ventaja_cs_rival", "mayor", None, None),
    ("ventaja_vision_rival", "mayor", None, None),
    ("wards_control", "mayor", None, None),
    ("wards_destruidas", "mayor", None, None),
    ("deaths", "menor", None, None),
    ("tiempo_muerto_seg", "menor", None, None),
    ("solo_kills", "mayor", None, None),
]


def _agregados(expresiones) -> str:
    return ",\n            ".join(
        f"avg({e}) AS {m}_avg, stddev_samp({e}) AS {m}_sd, count({e}) AS {m}_n"
        for m, e in expresiones
    )


@mcp.tool()
def get_coaching_priorities(
    player: str | None = None,
    days: int = 90,
    rol: str | None = None,
    solo_only: bool = True,
) -> dict:
    """
    Compara al jugador contra los otros jugadores de sus propias partidas
    y devuelve en qué métricas está por debajo, ordenadas por relevancia.

    El baseline son los demás participantes de sus partidas: el
    matchmaking los empareja al mismo MMR, así que representan el nivel
    al que ya juega. La comparación es siempre dentro del mismo rol.

    Cada métrica trae el tamaño de efecto (`d` de Cohen) y su
    `magnitud`. **Las prioridades se ordenan por tamaño de efecto, no
    por p-valor**: contra miles de partidas de pares casi todo sale
    significativo, así que el p-valor solo sirve como filtro y es el
    efecto el que dice qué importa.

    Esta herramienta entrega evidencia, no consejos: no infiere causas
    ni recomienda qué hacer. La síntesis de qué trabajar y en qué orden
    le corresponde al asistente, citando estos números. Conviene
    combinarla con get_trends para saber si una debilidad además está
    empeorando.

    Args:
        player: Riot ID ("nombre#tag"), nombre a secas, o vacío si solo
            hay un jugador rastreado.
        days: ventana hacia atrás en días.
        rol: TOP, JUNGLE, MIDDLE, BOTTOM o UTILITY. Si se omite, usa el
            rol más jugado en la ventana.
        solo_only: por defecto True, solo ranked solo/duo (cola 420).
            Flex queda fuera a propósito: es un modo menos serio y
            mezclarlo ensucia cualquier conclusión.
    """
    puuid = resolver_puuid(player)
    filtro_cola = "AND queue_id = 420" if solo_only else ""
    filtro = filtro_fecha(days)

    if rol is None:
        principales = run_query(f"""
            SELECT rol, count(*) AS partidas
            FROM matches_curated
            WHERE puuid = {sql_str(puuid)}
              AND {filtro}
              AND rol <> ''
              {filtro_cola}
            GROUP BY rol
            ORDER BY partidas DESC
            LIMIT 1
        """)
        if not principales:
            return {
                "error": "Sin partidas con rol asignado en la ventana pedida.",
                "ventana_dias": days,
            }
        rol = principales[0]["rol"]

    filas = run_query(f"""
        SELECT 'jugador' AS grupo, count(*) AS n, count(DISTINCT puuid) AS jugadores,
            {_agregados([(m, e_j or m) for m, _, e_j, _ in METRICAS_PARES])}
        FROM matches_curated
        WHERE puuid = {sql_str(puuid)}
          AND rol = {sql_str(rol)}
          AND {filtro}
          {filtro_cola}
        UNION ALL
        SELECT 'pares' AS grupo, count(*) AS n, count(DISTINCT puuid) AS jugadores,
            {_agregados([(m, e_p or m) for m, _, _, e_p in METRICAS_PARES])}
        FROM peers_curated
        WHERE rol = {sql_str(rol)}
          AND puuid <> {sql_str(puuid)}
          AND {filtro}
          {filtro_cola}
    """)

    grupos = {f["grupo"]: f for f in filas}
    yo, pares = grupos.get("jugador", {}), grupos.get("pares", {})
    n_yo, n_pares = int(yo.get("n") or 0), int(pares.get("n") or 0)

    if not n_yo or not n_pares:
        return {
            "error": f"Sin datos suficientes para el rol {rol} en la ventana pedida.",
            "rol": rol, "ventana_dias": days,
            "partidas_jugador": n_yo, "partidas_pares": n_pares,
        }

    comparaciones = []
    for nombre, sentido, _, _ in METRICAS_PARES:
        media_yo, media_pares = yo.get(f"{nombre}_avg"), pares.get(f"{nombre}_avg")
        if media_yo is None or media_pares is None:
            continue
        media_yo, media_pares = float(media_yo), float(media_pares)
        sd_yo = float(yo[f"{nombre}_sd"]) if yo.get(f"{nombre}_sd") else None
        sd_pares = float(pares[f"{nombre}_sd"]) if pares.get(f"{nombre}_sd") else None
        n_m_yo = int(yo.get(f"{nombre}_n") or 0)
        n_m_pares = int(pares.get(f"{nombre}_n") or 0)

        p = estadistica.comparar_medias(
            media_yo, sd_yo, n_m_yo, media_pares, sd_pares, n_m_pares
        )
        d = estadistica.tamano_efecto(
            media_yo, sd_yo, n_m_yo, media_pares, sd_pares, n_m_pares
        )
        delta = media_yo - media_pares
        comparaciones.append({
            "metrica": nombre,
            "jugador": round(media_yo, 2),
            "pares": round(media_pares, 2),
            "delta": round(delta, 2),
            "delta_pct": round(100.0 * delta / media_pares, 1) if media_pares else None,
            "d_cohen": _redondear(d, 2),
            "magnitud": estadistica.magnitud(d),
            "p_valor": _redondear(p, 4),
            "n_jugador": n_m_yo,
            "n_pares": n_m_pares,
            "direccion": _direccion(delta, sentido),
        })

    banderas = estadistica.ajustar_fdr([c["p_valor"] for c in comparaciones])
    for fila, significativa in zip(comparaciones, banderas):
        fila["significativo"] = significativa

    def relevante(c):
        # Una diferencia real pero minúscula no es una prioridad: se
        # exige que además supere el umbral de efecto chico.
        return c["significativo"] and c["d_cohen"] is not None and abs(c["d_cohen"]) >= 0.2

    peores = [c for c in comparaciones if relevante(c) and c["direccion"] == "empeora"]
    mejores = [c for c in comparaciones if relevante(c) and c["direccion"] == "mejora"]
    peores.sort(key=lambda c: abs(c["d_cohen"]), reverse=True)
    mejores.sort(key=lambda c: abs(c["d_cohen"]), reverse=True)

    if peores:
        nota = (f"{len(peores)} métrica(s) por debajo de los pares de {rol} con "
                "efecto al menos chico, ordenadas por tamaño de efecto. El orden "
                "es la prioridad sugerida; las causas y el plan los pone el "
                "asistente, no esta herramienta.")
    else:
        nota = (f"Ninguna métrica queda por debajo de los pares de {rol} con "
                "efecto relevante: el rendimiento está a la altura del nivel "
                "en el que juega.")

    return {
        "jugador": player or "(único rastreado)",
        "rol": rol,
        "ventana_dias": days,
        "solo_only": solo_only,
        "partidas_jugador": n_yo,
        "partidas_pares": n_pares,
        "jugadores_pares": int(pares.get("jugadores") or 0),
        "prioridades": peores,
        "fortalezas": mejores,
        "sin_diferencia_relevante": [
            c["metrica"] for c in comparaciones if not relevante(c)
        ],
        "nota": nota,
    }


# Minutos de referencia para la fase de líneas.
#
# 10 y 14 son los cortes habituales: el primero marca el final de la fase
# de líneas pura, el segundo el momento en que caen las torres exteriores
# y el mapa se abre. El 20 sirve para ver si una ventaja de laneo se
# sostiene o se diluye.
MINUTOS_REFERENCIA = (5, 10, 14, 20)


@mcp.tool()
def get_laning_benchmarks(
    player: str | None = None,
    days: int = 90,
    rol: str | None = None,
    solo_only: bool = True,
) -> dict:
    """
    Cómo va el jugador contra su rival directo de línea en los minutos
    clave, promediado sobre sus partidas.

    Sale del timeline, que es lo único que permite ver la partida antes
    de que termine: el resumen solo da totales finales. Las diferencias
    son contra el rival de la misma posición, así que ya vienen
    normalizadas por el ritmo de la partida.

    Un `diff_oro` negativo en el minuto 10 y positivo en el 20 describe a
    alguien que pierde la línea pero remonta después; el patrón inverso,
    a alguien que gana la línea y no la capitaliza. Esa lectura le
    corresponde al asistente: esta herramienta solo entrega las curvas.

    Args:
        player: Riot ID ("nombre#tag"), nombre a secas, o vacío si solo
            hay un jugador rastreado.
        days: ventana hacia atrás en días.
        rol: TOP, JUNGLE, MIDDLE, BOTTOM o UTILITY. Si se omite, agrupa
            por rol y devuelve todos.
        solo_only: por defecto True, solo ranked solo/duo (cola 420).
            Flex queda fuera a propósito: es un modo menos serio y
            mezclarlo ensucia cualquier conclusión.
    """
    puuid = resolver_puuid(player)

    # timeline_frames no guarda queue_id ni fecha: se filtran cruzando
    # con matches_curated, que sí los tiene y es una tabla diminuta.
    filtro_cola = "AND m.queue_id = 420" if solo_only else ""
    filtro_rol = f"AND f.rol = {sql_str(rol)}" if rol else ""
    minutos = ", ".join(str(m) for m in MINUTOS_REFERENCIA)

    filas = run_query(f"""
        SELECT
            f.rol,
            f.minuto,
            count(*) AS partidas,
            round(avg(CAST(f.cs AS DOUBLE)), 1)        AS cs,
            round(avg(CAST(f.cs_rival AS DOUBLE)), 1)  AS cs_rival,
            round(avg(CAST(f.diff_cs AS DOUBLE)), 1)   AS diff_cs,
            round(avg(CAST(f.oro AS DOUBLE)), 0)       AS oro,
            round(avg(CAST(f.diff_oro AS DOUBLE)), 0)  AS diff_oro,
            round(stddev_samp(CAST(f.diff_oro AS DOUBLE)), 0) AS diff_oro_sd,
            round(avg(CAST(f.diff_xp AS DOUBLE)), 0)   AS diff_xp,
            round(avg(CAST(f.nivel AS DOUBLE)), 1)     AS nivel,
            round(100.0 * sum(CASE WHEN f.diff_oro > 0 THEN 1 ELSE 0 END)
                  / count(*), 1)                       AS pct_por_delante
        FROM timeline_frames f
        JOIN matches_curated m
          ON m.match_id = f.match_id AND m.puuid = f.puuid
        WHERE f.puuid = {sql_str(puuid)}
          AND f.minuto IN ({minutos})
          AND f.oro_rival IS NOT NULL
          AND {filtro_fecha(days, alias="m")}
          {filtro_cola}
          {filtro_rol}
        GROUP BY f.rol, f.minuto
        ORDER BY f.rol, f.minuto
    """)

    if not filas:
        return {
            "error": "Sin frames de timeline para esa ventana. Puede que los "
                     "timelines de esas partidas aún no se hayan descargado.",
            "ventana_dias": days,
        }

    por_rol: dict[str, list[dict]] = {}
    for fila in coercer(filas):
        por_rol.setdefault(fila.pop("rol"), []).append(fila)

    return {
        "jugador": player or "(único rastreado)",
        "ventana_dias": days,
        "solo_only": solo_only,
        "minutos_referencia": list(MINUTOS_REFERENCIA),
        "por_rol": por_rol,
        "nota": ("diff_* es la diferencia contra el rival directo de línea: "
                 "positivo va por delante. pct_por_delante dice en qué "
                 "porcentaje de partidas iba ganando en oro a ese minuto, "
                 "que distingue una ventaja constante de una inflada por "
                 "pocas partidas muy buenas."),
    }


if __name__ == "__main__":
    mcp.run()
