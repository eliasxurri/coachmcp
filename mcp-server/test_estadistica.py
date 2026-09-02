"""
Verificación de las pruebas de significancia.

Sin scipy con qué contrastar, se comparan contra valores conocidos de
forma cerrada y contra tablas t estándar. Un p-valor mal calculado no
rompe nada visiblemente: haría que el asistente reportara rachas como
progreso, que es justo lo que la capa de tendencias intenta evitar.

    python3 test_estadistica.py
"""

import math
import sys

import estadistica as e

fallos = []


def check(descripcion, obtenido, esperado, tolerancia=5e-4):
    ok = obtenido is not None and abs(obtenido - esperado) <= tolerancia
    print(f"{'ok  ' if ok else 'FALLA'} {descripcion}: {obtenido} (esperado {esperado})")
    if not ok:
        fallos.append(descripcion)


print("--- distribución t contra formas cerradas ---")
# df=1 es una Cauchy: P(|T| > 1) = 0.5 exacto.
check("t(1) en t=1", e.p_valor_t(1, 1), 0.5)
# df=2 tiene forma cerrada: P(|T| > t) = 1 - t/sqrt(2 + t²).
check("t(2) en t=2", e.p_valor_t(2, 2), 1 - 2 / math.sqrt(6))
# Con muchos grados de libertad converge a la normal.
check("t(100000) en t=1.96", e.p_valor_t(1.96, 100_000), 0.05, tolerancia=1e-3)

print("\n--- distribución t contra tabla estándar (p=0.05 bilateral) ---")
for df, t_critico in [(5, 2.571), (10, 2.228), (20, 2.086), (30, 2.042), (100, 1.984)]:
    check(f"t({df}) en t={t_critico}", e.p_valor_t(t_critico, df), 0.05, tolerancia=1e-3)

print("\n--- normal estándar ---")
check("z=1.96", e.p_valor_normal(1.96), 0.05, tolerancia=1e-3)
check("z=1.0", e.p_valor_normal(1.0), 0.3173)
check("z=2.576", e.p_valor_normal(2.576), 0.01, tolerancia=1e-3)
check("z=0 (sin diferencia)", e.p_valor_normal(0.0), 1.0)

print("\n--- comparación de medias (Welch) ---")
# Muestras idénticas: sin evidencia de diferencia.
check("medias iguales", e.comparar_medias(5.0, 2.0, 30, 5.0, 2.0, 30), 1.0)
# Diferencia enorme con varianza chica: p prácticamente nulo.
p = e.comparar_medias(10.0, 1.0, 50, 5.0, 1.0, 50)
print(f"{'ok  ' if p < 1e-6 else 'FALLA'} diferencia grande: p={p:.2e} (esperado < 1e-6)")
if p >= 1e-6:
    fallos.append("diferencia grande")
# Welch con varianzas muy distintas no debe explotar.
p = e.comparar_medias(10.0, 20.0, 15, 8.0, 1.0, 60)
print(f"{'ok  ' if 0 <= p <= 1 else 'FALLA'} varianzas dispares: p={p:.4f} (esperado en [0,1])")
if not 0 <= p <= 1:
    fallos.append("varianzas dispares")

print("\n--- casos borde de medias ---")
for descripcion, resultado in [
    ("n=1 en una ventana", e.comparar_medias(5.0, 1.0, 1, 6.0, 1.0, 30)),
    ("media faltante", e.comparar_medias(None, None, 10, 6.0, 1.0, 30)),
]:
    ok = resultado is None
    print(f"{'ok  ' if ok else 'FALLA'} {descripcion}: {resultado} (esperado None)")
    if not ok:
        fallos.append(descripcion)

# Desviación cero en ambas: idénticas o distintas con certeza.
check("sd=0 e iguales", e.comparar_medias(5.0, 0.0, 10, 5.0, 0.0, 10), 1.0)
check("sd=0 y distintas", e.comparar_medias(6.0, 0.0, 10, 5.0, 0.0, 10), 0.0)

print("\n--- comparación de proporciones ---")
check("50/100 vs 50/100", e.comparar_proporciones(50, 100, 50, 100), 1.0)
# 70% vs 30% con n=100 cada uno: diferencia contundente.
p = e.comparar_proporciones(70, 100, 30, 100)
print(f"{'ok  ' if p < 1e-6 else 'FALLA'} 70% vs 30%: p={p:.2e} (esperado < 1e-6)")
if p >= 1e-6:
    fallos.append("70% vs 30%")
# El caso que motiva todo esto: 3/5 vs 2/5 NO es señal.
p = e.comparar_proporciones(3, 5, 2, 5)
print(f"{'ok  ' if p > 0.4 else 'FALLA'} 60% vs 40% con n=5: p={p:.4f} (esperado > 0.4)")
if p <= 0.4:
    fallos.append("muestra chica sin señal")

print("\n--- casos borde de proporciones ---")
for descripcion, resultado, esperado in [
    ("ventana vacía", e.comparar_proporciones(5, 10, 0, 0), None),
    ("nunca ganó", e.comparar_proporciones(0, 10, 0, 10), 1.0),
    ("siempre ganó", e.comparar_proporciones(10, 10, 10, 10), 1.0),
]:
    ok = resultado == esperado
    print(f"{'ok  ' if ok else 'FALLA'} {descripcion}: {resultado} (esperado {esperado})")
    if not ok:
        fallos.append(descripcion)

print("\n--- tamaño de efecto (d de Cohen) ---")
# Con varianzas iguales, d es la diferencia dividida por esa desviación.
check("una desviación de diferencia", e.tamano_efecto(6.0, 2.0, 50, 4.0, 2.0, 50), 1.0)
check("media desviación", e.tamano_efecto(5.0, 2.0, 50, 4.0, 2.0, 50), 0.5)
check("sin diferencia", e.tamano_efecto(5.0, 2.0, 50, 5.0, 2.0, 50), 0.0)
check("signo negativo", e.tamano_efecto(3.0, 2.0, 50, 5.0, 2.0, 50), -1.0)

# El punto de usar d: no depende del tamaño de muestra, el p-valor sí.
d_chico = e.tamano_efecto(5.1, 2.0, 30, 5.0, 2.0, 30)
d_grande = e.tamano_efecto(5.1, 2.0, 10_000, 5.0, 2.0, 10_000)
p_chico = e.comparar_medias(5.1, 2.0, 30, 5.0, 2.0, 30)
p_grande = e.comparar_medias(5.1, 2.0, 10_000, 5.0, 2.0, 10_000)
ok = abs(d_chico - d_grande) < 1e-6 and p_grande < 0.05 < p_chico
print(f"{'ok  ' if ok else 'FALLA'} d estable con n (d={d_chico:.4f} vs {d_grande:.4f}) "
      f"mientras el p-valor cae ({p_chico:.3f} -> {p_grande:.3f})")
if not ok:
    fallos.append("d independiente de n")

for descripcion, resultado in [
    ("muestra insuficiente", e.tamano_efecto(5.0, 1.0, 1, 4.0, 1.0, 30)),
    ("sin varianza", e.tamano_efecto(5.0, 0.0, 30, 4.0, 0.0, 30)),
]:
    ok = resultado is None
    print(f"{'ok  ' if ok else 'FALLA'} {descripcion}: {resultado} (esperado None)")
    if not ok:
        fallos.append(descripcion)

for d, esperado in [(0.1, "insignificante"), (0.3, "chico"), (0.6, "mediano"),
                    (1.2, "grande"), (-0.9, "grande"), (None, "indeterminado")]:
    obtenido = e.magnitud(d)
    ok = obtenido == esperado
    print(f"{'ok  ' if ok else 'FALLA'} magnitud({d}) = {obtenido} (esperado {esperado})")
    if not ok:
        fallos.append(f"magnitud({d})")

print("\n--- corrección de Benjamini-Hochberg ---")
# Ninguno pasa: el menor (0.04) necesitaría estar bajo 1/4*0.05 = 0.0125.
res = e.ajustar_fdr([0.04, 0.30, 0.60, 0.90])
ok = res == [False] * 4
print(f"{'ok  ' if ok else 'FALLA'} un p<0.05 aislado entre 4 pruebas: {res} (esperado todos False)")
if not ok:
    fallos.append("BH aislado")

# Todos muy chicos: todos se rechazan.
res = e.ajustar_fdr([0.001, 0.002, 0.003, 0.004])
ok = res == [True] * 4
print(f"{'ok  ' if ok else 'FALLA'} cuatro p muy chicos: {res} (esperado todos True)")
if not ok:
    fallos.append("BH todos")

# Caso escalonado: p=0.01 pasa (1/4*0.05=0.0125) y arrastra al resto por debajo.
res = e.ajustar_fdr([0.01, 0.02, 0.5, 0.6])
ok = res == [True, True, False, False]
print(f"{'ok  ' if ok else 'FALLA'} escalonado: {res} (esperado [True, True, False, False])")
if not ok:
    fallos.append("BH escalonado")

# Los None no se marcan nunca y no descuadran las posiciones.
res = e.ajustar_fdr([0.001, None, 0.002])
ok = res == [True, False, True]
print(f"{'ok  ' if ok else 'FALLA'} con None intercalado: {res} (esperado [True, False, True])")
if not ok:
    fallos.append("BH con None")

res = e.ajustar_fdr([None, None])
ok = res == [False, False]
print(f"{'ok  ' if ok else 'FALLA'} todos None: {res} (esperado [False, False])")
if not ok:
    fallos.append("BH todos None")

# Una sola prueba: BH se reduce al umbral simple.
ok = e.ajustar_fdr([0.04]) == [True] and e.ajustar_fdr([0.06]) == [False]
print(f"{'ok  ' if ok else 'FALLA'} prueba única equivale al umbral simple")
if not ok:
    fallos.append("BH prueba única")

print("\n--- umbral de muestra ---")
for descripcion, resultado, esperado in [
    ("10 y 10 alcanzan", e.muestra_suficiente(10, 10), True),
    ("9 no alcanza", e.muestra_suficiente(9, 40), False),
    ("ventana vacía", e.muestra_suficiente(0, 40), False),
]:
    ok = resultado is esperado
    print(f"{'ok  ' if ok else 'FALLA'} {descripcion}: {resultado} (esperado {esperado})")
    if not ok:
        fallos.append(descripcion)

print()
if fallos:
    print(f"FALLARON {len(fallos)} comprobaciones: {', '.join(fallos)}")
    sys.exit(1)
print("Todas las comprobaciones pasaron.")
