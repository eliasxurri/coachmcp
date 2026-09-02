-- Consultas de ejemplo sobre el data lake.
--
-- Todas filtran por partición (puuid, year, month) siempre que es
-- posible: Athena cobra por TB escaneado y las particiones son lo que
-- evita leer el bucket completo en cada consulta.
--
-- Las consultas 1-4 van contra matches_raw (JSON crudo). Para el uso
-- diario conviene la capa curada en Parquet (consultas 5 en adelante):
-- sobre las mismas 800 partidas, el winrate por campeón escanea 75 MB
-- en raw y 2.5 KB en curated. La capa raw sigue siendo útil para
-- campos que la curada no extrae todavía.

-- ---------------------------------------------------------------------
-- 1. Partidas ingeridas por día
-- ---------------------------------------------------------------------
SELECT
    year, month, day,
    count(*) AS partidas
FROM matches_raw
WHERE puuid = '<TU_PUUID>'
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC;


-- ---------------------------------------------------------------------
-- 2. Estadísticas propias por partida
--
-- El detalle de Riot trae los 10 jugadores en info.participants.
-- Este UNNEST expande ese array y filtra al jugador de interés.
-- ---------------------------------------------------------------------
WITH participantes AS (
    SELECT
        match_id,
        from_unixtime(game_creation / 1000) AS jugada_en,
        game_duration / 60.0 AS minutos,
        queue_id,
        participante
    FROM matches_raw
    CROSS JOIN UNNEST(
        CAST(json_extract(payload, '$.info.participants') AS ARRAY(JSON))
    ) AS t(participante)
    WHERE puuid = '<TU_PUUID>'
      AND year = '2026'
      AND month = '09'
)
SELECT
    jugada_en,
    json_extract_scalar(participante, '$.championName')  AS campeon,
    json_extract_scalar(participante, '$.teamPosition')  AS rol,
    CAST(json_extract_scalar(participante, '$.win') AS BOOLEAN) AS victoria,
    CAST(json_extract_scalar(participante, '$.kills')   AS INT) AS k,
    CAST(json_extract_scalar(participante, '$.deaths')  AS INT) AS d,
    CAST(json_extract_scalar(participante, '$.assists') AS INT) AS a,
    round(minutos, 1) AS duracion_min
FROM participantes
WHERE json_extract_scalar(participante, '$.puuid') = '<TU_PUUID>'
ORDER BY jugada_en DESC;


-- ---------------------------------------------------------------------
-- 3. Winrate y KDA por campeón
--
-- Esta es la consulta que alimentaría la tool get_champion_stats
-- del servidor MCP en la fase 2.
-- ---------------------------------------------------------------------
WITH mis_partidas AS (
    SELECT participante
    FROM matches_raw
    CROSS JOIN UNNEST(
        CAST(json_extract(payload, '$.info.participants') AS ARRAY(JSON))
    ) AS t(participante)
    WHERE puuid = '<TU_PUUID>'
      AND queue_id = 420  -- solo ranked solo/duo
)
SELECT
    json_extract_scalar(participante, '$.championName') AS campeon,
    count(*) AS partidas,
    round(
        100.0 * sum(
            CASE WHEN json_extract_scalar(participante, '$.win') = 'true'
                 THEN 1 ELSE 0 END
        ) / count(*),
        1
    ) AS winrate_pct,
    round(avg(CAST(json_extract_scalar(participante, '$.kills')   AS DOUBLE)), 1) AS kills_prom,
    round(avg(CAST(json_extract_scalar(participante, '$.deaths')  AS DOUBLE)), 1) AS deaths_prom,
    round(avg(CAST(json_extract_scalar(participante, '$.assists') AS DOUBLE)), 1) AS assists_prom,
    round(
        (avg(CAST(json_extract_scalar(participante, '$.kills')   AS DOUBLE)) +
         avg(CAST(json_extract_scalar(participante, '$.assists') AS DOUBLE)))
        / nullif(avg(CAST(json_extract_scalar(participante, '$.deaths') AS DOUBLE)), 0),
        2
    ) AS kda
FROM mis_partidas
WHERE json_extract_scalar(participante, '$.puuid') = '<TU_PUUID>'
GROUP BY json_extract_scalar(participante, '$.championName')
HAVING count(*) >= 3
ORDER BY partidas DESC;


-- ---------------------------------------------------------------------
-- 4. Registrar particiones si se cargaron datos fuera del pipeline
-- ---------------------------------------------------------------------
MSCK REPAIR TABLE matches_raw;


-- =====================================================================
-- CAPA CURADA (Parquet)
--
-- Una fila por jugador rastreado y partida, con los campos ya
-- extraídos: sin UNNEST y sin json_extract.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 5. Últimas partidas
-- ---------------------------------------------------------------------
SELECT
    jugada_en, campeon, rol, victoria,
    kills, deaths, assists, cs, oro, dano_a_campeones, vision_score,
    duracion_min
FROM matches_curated
WHERE puuid = '<TU_PUUID>'
ORDER BY jugada_en DESC
LIMIT 20;


-- ---------------------------------------------------------------------
-- 6. Winrate y KDA por campeón
--
-- Equivalente a la consulta 3, pero sobre la capa curada. Es la que usa
-- la tool get_champion_stats del servidor MCP.
--
-- El KDA, la participación en kills y las tasas por minuto vienen ya
-- calculadas por Riot en `challenges`: no hay que derivarlas.
-- ---------------------------------------------------------------------
SELECT
    campeon,
    count(*) AS partidas,
    round(100.0 * sum(CASE WHEN victoria THEN 1 ELSE 0 END) / count(*), 1) AS winrate_pct,
    round(avg(CAST(kills   AS DOUBLE)), 1) AS kills_prom,
    round(avg(CAST(deaths  AS DOUBLE)), 1) AS deaths_prom,
    round(avg(CAST(assists AS DOUBLE)), 1) AS assists_prom,
    round(avg(kda), 2)                     AS kda,
    round(avg(participacion_kills), 3)     AS participacion_kills,
    round(avg(pct_dano_equipo), 3)         AS pct_dano_equipo,
    round(avg(CAST(cs AS DOUBLE) / nullif(duracion_min, 0)), 1) AS cs_por_min,
    round(avg(dano_por_min), 1)            AS dano_por_min,
    round(avg(oro_por_min), 1)             AS oro_por_min,
    round(avg(CAST(vision_score AS DOUBLE)), 1) AS vision_prom,
    round(avg(CAST(tiempo_muerto_seg AS DOUBLE)), 0) AS seg_muerto
FROM matches_curated
WHERE puuid = '<TU_PUUID>'
  AND queue_id = 420  -- solo ranked solo/duo
GROUP BY campeon
HAVING count(*) >= 5
ORDER BY partidas DESC;


-- ---------------------------------------------------------------------
-- 7. Rendimiento por rol
--
-- Útil para detectar en qué posición conviene concentrarse.
-- ---------------------------------------------------------------------
SELECT
    rol,
    count(*) AS partidas,
    round(100.0 * sum(CASE WHEN victoria THEN 1 ELSE 0 END) / count(*), 1) AS winrate_pct,
    round(avg(CAST(cs AS DOUBLE) / nullif(duracion_min, 0)), 1) AS cs_por_min,
    round(avg(CAST(vision_score AS DOUBLE)), 1) AS vision_prom
FROM matches_curated
WHERE puuid = '<TU_PUUID>'
  AND rol <> ''
GROUP BY rol
ORDER BY partidas DESC;


-- ---------------------------------------------------------------------
-- 8. Comparación de dos ventanas de 30 días
--
-- Es la consulta que alimenta la tool get_trends. Devuelve promedios y
-- desviaciones estándar por ventana; la desviación es lo que permite
-- después decidir si un cambio es significativo o ruido (el servidor MCP
-- corre una t de Welch sobre estos números).
--
-- CS, daño y oro van por minuto: si la duración media cambia entre
-- ventanas, los totales miden duración tanto como desempeño.
-- ---------------------------------------------------------------------
WITH ventanas AS (
    SELECT
        CASE WHEN jugada_en >= current_timestamp - INTERVAL '30' DAY
             THEN 'reciente' ELSE 'previo' END AS ventana,
        CASE WHEN victoria THEN 1 ELSE 0 END AS win,
        CAST(kills   AS DOUBLE) AS kills,
        CAST(deaths  AS DOUBLE) AS deaths,
        CAST(assists AS DOUBLE) AS assists,
        CAST(cs AS DOUBLE) / nullif(duracion_min, 0) AS cs_por_min,
        CAST(vision_score AS DOUBLE) AS vision_score,
        CAST(dano_a_campeones AS DOUBLE) / nullif(duracion_min, 0) AS dano_por_min,
        CAST(oro AS DOUBLE) / nullif(duracion_min, 0) AS oro_por_min,
        duracion_min
    FROM matches_curated
    WHERE puuid = '<TU_PUUID>'
      AND jugada_en >= current_timestamp - INTERVAL '60' DAY
)
SELECT
    ventana,
    count(*) AS partidas,
    round(100.0 * sum(win) / count(*), 1) AS winrate_pct,
    round(avg(deaths), 2)       AS deaths_prom,
    round(stddev_samp(deaths), 2) AS deaths_sd,
    round(avg(cs_por_min), 2)   AS cs_por_min,
    round(stddev_samp(cs_por_min), 2) AS cs_por_min_sd,
    round(avg(dano_por_min), 1) AS dano_por_min,
    round(avg(oro_por_min), 1)  AS oro_por_min,
    round(avg(vision_score), 1) AS vision_prom,
    round(avg(duracion_min), 1) AS duracion_prom
FROM ventanas
GROUP BY ventana
ORDER BY ventana DESC;


-- ---------------------------------------------------------------------
-- 9. Fase temprana por rol
--
-- Usa los campos de `challenges` que Riot ya calcula. Ojo: cs_primeros_10
-- cuenta súbditos DE LÍNEA, así que da casi cero en jungla; para ese rol
-- el equivalente es jungla_cs_antes_10.
--
-- Las columnas de ventaja sobre el rival solo existen cuando Riot
-- identifica un oponente directo (~77% de las partidas), de ahí el
-- conteo aparte para no promediar sobre filas que no existen.
-- ---------------------------------------------------------------------
SELECT
    rol,
    count(*) AS partidas,
    round(avg(CAST(cs_primeros_10 AS DOUBLE)), 1)      AS cs_linea_10min,
    round(avg(CAST(jungla_cs_antes_10 AS DOUBLE)), 1)  AS cs_jungla_10min,
    round(avg(CAST(placas_torre AS DOUBLE)), 1)        AS placas,
    count(ventaja_cs_rival)                            AS n_con_rival,
    round(avg(ventaja_cs_rival), 1)                    AS ventaja_cs,
    round(avg(ventaja_vision_rival), 2)                AS ventaja_vision,
    round(avg(CAST(tiempo_muerto_seg AS DOUBLE)), 0)   AS seg_muerto,
    round(avg(participacion_kills), 3)                 AS participacion_kills
FROM matches_curated
WHERE puuid = '<TU_PUUID>'
  AND rol <> ''
GROUP BY rol
ORDER BY partidas DESC;


-- ---------------------------------------------------------------------
-- 10. Rendimiento por parche
--
-- Un cambio de rendimiento que coincide con un cambio de parche tiene
-- otra explicación que un cambio de hábitos.
-- ---------------------------------------------------------------------
SELECT
    regexp_extract(parche, '^(\d+\.\d+)', 1) AS parche_corto,
    count(*) AS partidas,
    round(100.0 * sum(CASE WHEN victoria THEN 1 ELSE 0 END) / count(*), 1) AS winrate_pct,
    round(avg(kda), 2)          AS kda,
    round(avg(dano_por_min), 1) AS dano_por_min,
    round(avg(oro_por_min), 1)  AS oro_por_min
FROM matches_curated
WHERE puuid = '<TU_PUUID>'
GROUP BY regexp_extract(parche, '^(\d+\.\d+)', 1)
HAVING count(*) >= 10
-- Ordenar por texto pondría 16.9 por encima de 16.17: hay que comparar
-- mayor y menor como números.
ORDER BY
    CAST(split_part(regexp_extract(parche, '^(\d+\.\d+)', 1), '.', 1) AS INT) DESC,
    CAST(split_part(regexp_extract(parche, '^(\d+\.\d+)', 1), '.', 2) AS INT) DESC;


-- ---------------------------------------------------------------------
-- 11. Tendencia mensual
--
-- Vista larga del progreso, mes a mes.
-- ---------------------------------------------------------------------
SELECT
    date_format(jugada_en, '%Y-%m') AS mes,
    count(*) AS partidas,
    round(100.0 * sum(CASE WHEN victoria THEN 1 ELSE 0 END) / count(*), 1) AS winrate_pct,
    round(
        (avg(CAST(kills AS DOUBLE)) + avg(CAST(assists AS DOUBLE)))
        / nullif(avg(CAST(deaths AS DOUBLE)), 0),
        2
    ) AS kda
FROM matches_curated
WHERE puuid = '<TU_PUUID>'
GROUP BY date_format(jugada_en, '%Y-%m')
ORDER BY mes DESC;


-- =====================================================================
-- BASELINE DE PARES
--
-- Los otros participantes de las mismas partidas. El matchmaking los
-- empareja al mismo MMR, así que son una referencia del elo propio.
-- Siempre se compara dentro del mismo rol.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 12. Percentiles de los pares por rol
--
-- Da el contexto para leer cualquier número propio: si un jungla hace
-- 6,5 CS/min, esto dice si eso es del montón o destacado.
-- ---------------------------------------------------------------------
SELECT
    rol,
    count(*) AS observaciones,
    count(DISTINCT puuid) AS jugadores,
    round(approx_percentile(cs_por_min, 0.25), 2)     AS cs_min_p25,
    round(approx_percentile(cs_por_min, 0.50), 2)     AS cs_min_p50,
    round(approx_percentile(cs_por_min, 0.75), 2)     AS cs_min_p75,
    round(approx_percentile(participacion_kills, 0.50), 3) AS part_kills_p50,
    round(approx_percentile(dano_por_min, 0.50), 1)   AS dano_min_p50,
    round(approx_percentile(vision_por_min, 0.50), 2) AS vision_min_p50
FROM peers_curated
WHERE queue_id = 420
GROUP BY rol
ORDER BY observaciones DESC;


-- ---------------------------------------------------------------------
-- 13. Jugador contra sus pares del mismo rol
--
-- Es la consulta que alimenta get_coaching_priorities. El servidor MCP
-- toma estas medias y desviaciones y calcula la d de Cohen: las
-- prioridades se ordenan por tamaño de efecto, no por p-valor, porque
-- con miles de partidas de pares casi todo sale significativo.
-- ---------------------------------------------------------------------
SELECT 'jugador' AS grupo,
    count(*) AS n,
    round(avg(kda), 2)                  AS kda,
    round(avg(participacion_kills), 3)  AS participacion_kills,
    round(avg(CAST(cs AS DOUBLE) / nullif(duracion_min, 0)), 2) AS cs_por_min,
    round(stddev_samp(CAST(cs AS DOUBLE) / nullif(duracion_min, 0)), 2) AS cs_por_min_sd,
    round(avg(dano_por_min), 1)         AS dano_por_min,
    round(avg(CAST(wards_control AS DOUBLE)), 2) AS wards_control
FROM matches_curated
WHERE puuid = '<TU_PUUID>' AND rol = 'JUNGLE' AND queue_id = 420
UNION ALL
SELECT 'pares',
    count(*),
    round(avg(kda), 2),
    round(avg(participacion_kills), 3),
    round(avg(cs_por_min), 2),
    round(stddev_samp(cs_por_min), 2),
    round(avg(dano_por_min), 1),
    round(avg(CAST(wards_control AS DOUBLE)), 2)
FROM peers_curated
WHERE rol = 'JUNGLE' AND queue_id = 420 AND puuid <> '<TU_PUUID>';


-- =====================================================================
-- TIMELINE (minuto a minuto)
--
-- timeline_frames aplana el JSON del timeline a una fila por minuto,
-- con el jugador y su rival directo de línea lado a lado. Consultar
-- timelines_raw en crudo no es viable: 800 timelines pesan ~840 MB y
-- reventarían el corte de 100 MB del workgroup interactivo.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 14. Benchmarks de fase de líneas
--
-- Es la consulta que alimenta get_laning_benchmarks. pct_por_delante
-- distingue una ventaja constante de una inflada por pocas partidas
-- muy buenas: el promedio solo no lo dice.
-- ---------------------------------------------------------------------
SELECT
    f.rol,
    f.minuto,
    count(*) AS partidas,
    round(avg(CAST(f.cs AS DOUBLE)), 1)       AS cs,
    round(avg(CAST(f.diff_cs AS DOUBLE)), 1)  AS diff_cs,
    round(avg(CAST(f.diff_oro AS DOUBLE)), 0) AS diff_oro,
    round(avg(CAST(f.diff_xp AS DOUBLE)), 0)  AS diff_xp,
    round(100.0 * sum(CASE WHEN f.diff_oro > 0 THEN 1 ELSE 0 END) / count(*), 1)
        AS pct_por_delante
FROM timeline_frames f
JOIN matches_curated m
  ON m.match_id = f.match_id AND m.puuid = f.puuid
WHERE f.puuid = '<TU_PUUID>'
  AND f.minuto IN (5, 10, 14, 20)
  AND f.oro_rival IS NOT NULL
  AND m.queue_id = 420
GROUP BY f.rol, f.minuto
ORDER BY f.rol, f.minuto;


-- ---------------------------------------------------------------------
-- 15. ¿La ventaja de laneo se convierte en victoria?
--
-- Cruza cómo iba el minuto 14 contra el resultado final. Si ganar la
-- línea no mueve el winrate, el problema no está en el laneo.
-- ---------------------------------------------------------------------
SELECT
    CASE
        WHEN diff_oro >  500 THEN 'adelante (+500)'
        WHEN diff_oro < -500 THEN 'atras (-500)'
        ELSE 'parejo'
    END AS estado_min14,
    count(*) AS partidas,
    round(100.0 * sum(CASE WHEN victoria THEN 1 ELSE 0 END) / count(*), 1) AS winrate_pct
FROM timeline_frames
WHERE puuid = '<TU_PUUID>'
  AND minuto = 14
  AND oro_rival IS NOT NULL
GROUP BY 1
ORDER BY partidas DESC;
