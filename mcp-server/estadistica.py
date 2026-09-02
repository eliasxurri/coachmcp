"""
Pruebas de significancia para comparar dos ventanas temporales.

El punto de estas funciones es evitar el error clásico de una detección
de tendencias: reportar ruido como si fuera señal. Con 5 partidas, pasar
de 40% a 60% de winrate es lo que se espera del azar, no una mejora.

Se implementan a mano (sin scipy) porque son pocas líneas y así el
servidor MCP no arrastra una dependencia de ~30 MB. La distribución t se
calcula exacta, no por aproximación normal: las muestras chicas son
justo el caso donde una tendencia falsa engañaría al asistente, así que
es donde menos conviene aproximar.
"""

import math

# Umbral de significancia. 0.05 es la convención; con las muestras de
# este caso de uso (decenas de partidas) es un compromiso razonable
# entre perder mejoras reales y anunciar rachas como si fueran progreso.
ALFA = 0.05


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Fracción continua de Lentz para la beta incompleta."""
    tiny = 1e-30
    qab, qap, qam = a + b, a + 1.0, a - 1.0

    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    resultado = d

    for m in range(1, 200):
        m2 = 2 * m

        # Paso par.
        numerador = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + numerador * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + numerador / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        resultado *= d * c

        # Paso impar.
        numerador = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + numerador * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + numerador / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        resultado *= delta

        if abs(delta - 1.0) < 3e-16:
            break

    return resultado


def beta_incompleta(a: float, b: float, x: float) -> float:
    """Beta incompleta regularizada I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0

    factor = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log1p(-x)
    )

    if x < (a + 1.0) / (a + b + 2.0):
        return factor * _beta_continued_fraction(a, b, x) / a
    return 1.0 - factor * _beta_continued_fraction(b, a, 1.0 - x) / b


def p_valor_t(t: float, grados_libertad: float) -> float:
    """P(|T| > |t|) para una t de Student. Bilateral."""
    if grados_libertad <= 0 or not math.isfinite(t):
        return 1.0
    return beta_incompleta(
        grados_libertad / 2.0, 0.5, grados_libertad / (grados_libertad + t * t)
    )


def p_valor_normal(z: float) -> float:
    """P(|Z| > |z|) para una normal estándar. Bilateral."""
    if not math.isfinite(z):
        return 1.0
    return math.erfc(abs(z) / math.sqrt(2.0))


def comparar_medias(
    media_a: float | None, sd_a: float | None, n_a: int,
    media_b: float | None, sd_b: float | None, n_b: int,
) -> float | None:
    """
    Prueba t de Welch entre dos muestras independientes.

    Se usa Welch y no la t clásica porque no se puede asumir que la
    varianza sea igual entre ventanas: jugar campeones distintos cambia
    la dispersión de casi todas las métricas.

    Devuelve el p-valor, o None si no hay datos suficientes.
    """
    if None in (media_a, media_b) or n_a < 2 or n_b < 2:
        return None

    var_a = (sd_a or 0.0) ** 2 / n_a
    var_b = (sd_b or 0.0) ** 2 / n_b
    error_estandar = math.sqrt(var_a + var_b)

    if error_estandar == 0.0:
        # Sin varianza: o son idénticas, o difieren con certeza.
        return 1.0 if media_a == media_b else 0.0

    t = (media_a - media_b) / error_estandar

    # Welch-Satterthwaite.
    denominador = (
        var_a**2 / (n_a - 1) + var_b**2 / (n_b - 1)
    )
    grados_libertad = (var_a + var_b) ** 2 / denominador if denominador else n_a + n_b - 2

    return p_valor_t(t, grados_libertad)


def comparar_proporciones(
    exitos_a: int, n_a: int, exitos_b: int, n_b: int
) -> float | None:
    """
    Prueba z de dos proporciones con varianza agrupada (para winrate).

    Devuelve el p-valor, o None si alguna ventana está vacía.
    """
    if n_a < 1 or n_b < 1:
        return None

    p_agrupada = (exitos_a + exitos_b) / (n_a + n_b)
    if p_agrupada in (0.0, 1.0):
        return 1.0  # nadie ganó nunca, o nadie perdió nunca: sin señal

    error_estandar = math.sqrt(p_agrupada * (1 - p_agrupada) * (1 / n_a + 1 / n_b))
    if error_estandar == 0.0:
        return 1.0

    z = (exitos_a / n_a - exitos_b / n_b) / error_estandar
    return p_valor_normal(z)


def tamano_efecto(
    media_a: float | None, sd_a: float | None, n_a: int,
    media_b: float | None, sd_b: float | None, n_b: int,
) -> float | None:
    """
    d de Cohen: la diferencia entre medias en unidades de desviación.

    Hace falta porque el p-valor mide *si* hay diferencia, no *cuánta*.
    Comparando contra miles de partidas de pares, una diferencia
    irrelevante sale significativa igual: ordenar prioridades por
    p-valor pondría primero lo que tiene más muestra, no lo que más
    afecta al juego. El tamaño de efecto no depende de la muestra.

    Convención: 0,2 chico · 0,5 mediano · 0,8 grande.
    """
    if None in (media_a, media_b) or n_a < 2 or n_b < 2:
        return None

    var_a, var_b = (sd_a or 0.0) ** 2, (sd_b or 0.0) ** 2
    sd_agrupada = math.sqrt(
        ((n_a - 1) * var_a + (n_b - 1) * var_b) / (n_a + n_b - 2)
    )
    if sd_agrupada == 0.0:
        return None
    return (media_a - media_b) / sd_agrupada


def magnitud(d: float | None) -> str:
    """Etiqueta legible para un tamaño de efecto."""
    if d is None:
        return "indeterminado"
    escala = abs(d)
    if escala < 0.2:
        return "insignificante"
    if escala < 0.5:
        return "chico"
    if escala < 0.8:
        return "mediano"
    return "grande"


def ajustar_fdr(p_valores: list[float | None], alfa: float = ALFA) -> list[bool]:
    """
    Corrección de Benjamini-Hochberg para comparaciones múltiples.

    Comparar 20 métricas a la vez con umbral 0,05 produce en promedio una
    "significativa" por puro azar. Sin esta corrección, agregar métricas
    a la comparación degrada en silencio la garantía de que un cambio
    marcado es real.

    Se usa Benjamini-Hochberg y no Bonferroni porque este último, al
    dividir el umbral entre el número de pruebas, escondería mejoras
    reales: aquí importa más no perderlas que blindarse contra un único
    falso positivo.

    Devuelve una lista de booleanos alineada con la entrada. Las
    posiciones con p-valor None nunca se marcan.
    """
    indexados = [(p, i) for i, p in enumerate(p_valores) if p is not None]
    resultado = [False] * len(p_valores)
    if not indexados:
        return resultado

    indexados.sort()
    m = len(indexados)

    # El mayor k cuyo p-valor cae bajo su umbral escalonado; se rechaza
    # todo lo anterior, aunque alguno individual no pase el umbral.
    corte = 0
    for k, (p, _) in enumerate(indexados, start=1):
        if p <= k / m * alfa:
            corte = k

    for _, indice in indexados[:corte]:
        resultado[indice] = True
    return resultado


def muestra_suficiente(n_a: int, n_b: int, minimo: int = 10) -> bool:
    """
    Si alguna ventana tiene menos de `minimo` partidas, cualquier
    conclusión es frágil aunque el p-valor salga bajo.
    """
    return min(n_a, n_b) >= minimo
