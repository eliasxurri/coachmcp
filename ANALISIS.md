# Hallazgos del análisis

Qué dicen los datos del jugador rastreado, y con qué nivel de confianza.
El README documenta *cómo está construido el sistema*; este documento
recoge *qué encontró*, con los números y sus límites.

Datos al 2026-09-02: **525 partidas de ranked solo/duo** entre
2025-03-24 y 2026-09-01, con 54,5% de victorias.

---

## Reglas del análisis

Cuatro reglas gobiernan todo lo que sigue. Cada una nació de un error
que se cometió primero y se corrigió después.

**Solo ranked solo/duo (cola 420).** Flex es un modo menos serio y ARAM
directamente no comparte las reglas del juego. Mezclarlos no era un
detalle: al recalcular las tendencias de 30 días solo con soloq, las
tres métricas que aparecían como significativas —CS, oro y daño por
minuto— dejaron de serlo. Eran un artefacto de promediar ARAM con
partidas de Grieta.

**Todo normalizado por minuto.** Los totales por partida miden cuánto
duró la partida tanto como cuán bien se jugó. Cuando la duración media
subió 9,7% entre dos ventanas, el oro por partida parecía plano
(p=0,78); por minuto, la caída del 10,2% aparecía con p=0,006.

**Corrección por comparaciones múltiples.** Comparar 23 métricas con
umbral 0,05 produce más de una "significativa" por azar en cada
consulta. Se aplica Benjamini-Hochberg y se reporta en tres niveles:
significativo, indicio y ruido.

**Prioridades por tamaño de efecto, no por p-valor.** Contra miles de
partidas de pares casi todo sale significativo; la d de Cohen dice qué
importa. El p-valor solo filtra el azar.

---

## Rendimiento por rol

| Rol | Partidas | Winrate |
|---|---|---|
| JUNGLE | 409 | 56,7% |
| UTILITY | 65 | 49,2% |
| MIDDLE | 33 | 48,5% |
| TOP | 17 | 35,3% |

Jungla es el rol principal por amplio margen y el único con muestra
suficiente para conclusiones firmes. Los otros roles quedan como
indicios: 17 partidas de top no distinguen "juego mal el rol" de una
mala racha.

---

## Contra jugadores del propio elo

El baseline son los otros participantes de las mismas partidas, que el
matchmaking empareja al mismo MMR. Comparación dentro del mismo rol.

**Histórico completo (409 partidas de jungla contra 641 de 610 pares):**
ninguna métrica queda por debajo de los pares con efecto relevante. Tres
fortalezas, todas de efecto chico: wards de control (+41,6%), ventaja de
CS sobre el rival (+35,3%) y CS por minuto (+5,9%).

**Últimos 60 días (112 propias contra 166 de pares):** aparece una
debilidad con efecto mediano.

| Métrica | Jugador | Pares | Δ | d de Cohen |
|---|---|---|---|---|
| **Participación en kills** | **0,48** | **0,55** | **−14,2%** | **−0,53** |
| Ventaja de CS sobre el rival | 39,39 | 21,40 | +84,1% | +0,61 |
| Wards de control | 3,31 | 2,10 | +57,6% | +0,57 |
| CS por minuto | 7,53 | 6,84 | +10,0% | +0,53 |

El patrón es coherente y es el hallazgo principal: **farmea y controla
visión por encima de su elo, pero aparece en menos peleas**. En jungla
eso suele significar rutas de farmeo que no convierten la ventaja en
presión sobre el mapa.

---

## Fase de líneas

Contra el rival directo de la misma posición, sacado del timeline
(minuto a minuto). `%delante` es en qué porcentaje de partidas iba
ganando en oro a ese minuto, y distingue una ventaja constante de una
inflada por pocas partidas muy buenas.

| Rol | n | ΔCS@14 | ΔOro@14 | ΔOro@20 | %delante@20 |
|---|---|---|---|---|---|
| JUNGLE | 389 | +3,2 | −48 | +83 | 53% |
| UTILITY | 60 | −1,3 | +67 | −3 | 44% |
| MIDDLE | 31 | −4,8 | −280 | −376 | 38% |
| TOP | 17 | −18,6 | −885 | −1193 | 29% |

En jungla la fase temprana está exactamente pareja y la ventaja aparece
después del minuto 20. Fuera de rol el déficit es claro desde el
principio, aunque la muestra de top y mid es chica.

**La ventaja temprana sí se convierte en victorias** (525 partidas,
corte en el minuto 14):

| Estado al minuto 14 | Partidas | Winrate |
|---|---|---|
| Adelante (+500 oro) | 154 | 62,3% |
| Parejo | 179 | 57,5% |
| Atrás (−500 oro) | 165 | 43,6% |

Casi 20 puntos de diferencia entre los extremos: el laneo no es un
problema aislado del resultado.

---

## Tendencias recientes

Comparando los últimos 30 días contra los 30 previos (87 contra 52
partidas de soloq), de 23 métricas comparadas **una sola** sobrevive la
corrección por comparaciones múltiples:

- **Wards de control: −32,6%** (2,85 contra 4,23 por partida, p=0,0014).

Quedan como indicio, que valen como pista y no como conclusión: solo
kills (−40,2%, p=0,021) y CS en los primeros 10 minutos (+144,3%,
p=0,0073).

El winrate bajó 2,8 puntos en ese período con **p=0,71**: indistinguible
del azar. Un tablero cualquiera lo habría reportado como un bajón.

**Cambio de pool:** 45 de las 87 partidas recientes son de campeones que
no aparecían en la ventana anterior (Sylas 16, Vi 7, Kayn 6, TwistedFate
4). Un cambio de pool de esa magnitud explica buena parte del ruido en
las métricas agregadas.

---

## ¿Los nerfeos del parche explican la caída?

La hipótesis era que los nerfeos del parche 16.17 a Nocturne y al
itemizado de Wukong explicaban la caída de winrate. Se verificó contra
**Data Dragon**, la fuente estática oficial de Riot: gratis, sin API
key, con 498 versiones de histórico.

**Los cambios son reales y están documentados:**

| Qué | 16.16 | 16.17 |
|---|---|---|
| Nocturne, vida base | 655 | **640** |
| Nocturne, armadura base | 38 | **36** |
| Sundered Sky, vida | 450 | **400** |
| Sundered Sky, daño de ataque | 45 | **40** |

La hipótesis del encarecimiento no se sostiene: Sundered Sky cuesta
3.100 oro sin cambios desde el parche 16.10, y Divine Sunderer 3.450
igualmente sin cambios. El nerfeo fue a las estadísticas, no al precio.

**Pero la caída de winrate no se puede atribuir a esos nerfeos.** En
16.17 hay 12 partidas con 25% de victorias, frente a 66,7% en 16.16.
Aislado, ese contraste da p=0,0355. El problema es que ese parche se
eligió *después* de ver que era el peor, que es el problema de
comparaciones múltiples disfrazado.

Al probar los 18 parches con al menos 10 partidas contra el resto del
historial y corregir por Benjamini-Hochberg, **ninguno resulta
significativo**:

| Parche | n | Winrate | p | Tras corregir |
|---|---|---|---|---|
| 16.17 | 12 | 25,0% | 0,0355 | ruido |
| 16.10 | 19 | 31,6% | 0,0378 | ruido |
| 16.11 | 17 | 35,3% | 0,0994 | ruido |
| 16.16 | 39 | 66,7% | 0,1220 | ruido |
| … (14 más) | | | >0,2 | ruido |

El 16.10 muestra una caída casi idéntica sin ningún nerfeo asociado.
Oscilaciones de esta magnitud son normales en este historial.

**Conclusión honesta:** los nerfeos ocurrieron y afectan al pool, pero
con 12 partidas no se puede separar su efecto del ruido. Haría falta
esperar a acumular partidas en el parche, o un winrate global del
campeón como control externo, que Riot no publica por API.

---

## Limitaciones conocidas

**No hay winrates globales.** Riot nunca los expuso por API; sitios como
op.gg los calculan agregando millones de partidas. Scrapearlos va contra
sus términos y están protegidos por Cloudflare. Data Dragon cubre *qué
cambió* en cada parche, no *cuánto afectó*.

**El baseline de pares no sirve por campeón.** Nocturne tiene 29
partidas de control repartidas en 15 parches, y está sesgado por
construcción: son jugadores de las partidas propias, donde el jugador
gana el 56%, así que los rivales aparecen con winrate deprimido.

**Roles secundarios sin muestra.** Top (17), mid (33) y support (65) dan
indicios, no conclusiones.

**Las métricas contra el rival de línea están casi completas donde
importa.** Cubren el 95,4% de las partidas de soloq, con la misma
cobertura en todos los roles: jungla 95,4%, mid 97%, top 100%. Las
ausencias son de modos sin líneas —Arena, ARAM, URF— y de remakes de
menos de dos minutos. Aun así las consultas cuentan cada métrica por
separado y marcan las filas con `cobertura_parcial`.
