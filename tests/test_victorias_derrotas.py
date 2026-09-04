"""
Comparar victorias contra derrotas tiene una trampa: en una derrota casi
todo sale peor, porque perder y tener malos números son el mismo hecho.

Lo que informa es el orden por tamaño de efecto y, sobre todo, qué NO
difiere: eso descarta fases como causa. Estas pruebas fijan que la
herramienta empuje esa lectura en vez de la ingenua.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "mcp-server"))

with patch("boto3.client"), patch("boto3.resource"):
    import server


def agregados(medias_victoria: dict, medias_derrota: dict, n=60):
    filas = []
    for resultado, medias in (("victoria", medias_victoria), ("derrota", medias_derrota)):
        fila = {"resultado": resultado, "n": str(n)}
        for nombre, _, tipo, _ in server.METRICAS_RESULTADO:
            if tipo == "proporcion":
                fila[f"{nombre}_si"] = str(int(medias.get(nombre, 0.5) * n))
                fila[f"{nombre}_n"] = str(n)
            else:
                fila[f"{nombre}_avg"] = str(medias.get(nombre, 5.0))
                fila[f"{nombre}_sd"] = "1.0"
                fila[f"{nombre}_n"] = str(n)
        filas.append(fila)
    return filas


class VictoriasDerrotasTests(unittest.TestCase):
    def _comparar(self, victoria, derrota):
        fn = getattr(server.get_win_loss_split, "fn", None) or server.get_win_loss_split
        with patch.object(server, "resolver_puuid", return_value="P" * 78), \
             patch.object(server, "run_query",
                          side_effect=[[{"rol": "JUNGLE", "partidas": "120"}],
                                       agregados(victoria, derrota)]):
            return fn(days=90)

    def test_lo_que_no_difiere_se_marca_como_descarte(self):
        r = self._comparar({}, {})
        self.assertTrue(r["iguales_en_ambas"])
        self.assertFalse(r["se_derrumban"])

    def test_ordena_por_tamano_de_efecto_y_no_por_p_valor(self):
        r = self._comparar(
            {"dragones_takedowns": 9.0, "cs_por_min": 5.4},
            {"dragones_takedowns": 5.0, "cs_por_min": 5.0},
        )
        metricas = [c["metrica"] for c in r["se_derrumban"]]
        self.assertEqual(metricas[0], "dragones_takedowns")

    def test_advierte_que_no_prueba_causalidad(self):
        r = self._comparar({"dragones_takedowns": 9.0}, {"dragones_takedowns": 5.0})
        self.assertIn("causalidad", r["nota"])
        principal = r["se_derrumban"][0]
        self.assertIn("NO afirmes que lo causa", principal["como_reportar"])

    def test_exige_muestra_en_ambos_lados(self):
        fn = getattr(server.get_win_loss_split, "fn", None) or server.get_win_loss_split
        filas = agregados({}, {})
        filas[1]["n"] = "2"   # solo 2 derrotas
        with patch.object(server, "resolver_puuid", return_value="P" * 78), \
             patch.object(server, "run_query",
                          side_effect=[[{"rol": "JUNGLE", "partidas": "62"}], filas]):
            r = fn(days=90)
        self.assertIn("insuficiente", r["error"])


class PrioridadesSinTendenciaTests(unittest.TestCase):
    def test_la_comparacion_con_pares_prohibe_hablar_de_tendencia(self):
        """
        El modelo afirmó que una métrica "venía empeorando" leyendo solo la
        comparación con pares, sin pedir tendencias. El aviso estaba en la
        nota general; tiene que estar pegado al número.
        """
        fn = getattr(server.get_coaching_priorities, "fn", None) or server.get_coaching_priorities
        filas = [
            {"grupo": "jugador", "n": "60", "jugadores": "1"},
            {"grupo": "pares", "n": "400", "jugadores": "380"},
        ]
        for fila in filas:
            for nombre, _, _, _ in server.METRICAS_PARES:
                propio = fila["grupo"] == "jugador"
                fila[f"{nombre}_avg"] = "4.0" if propio else "5.0"
                fila[f"{nombre}_sd"] = "1.0"
                fila[f"{nombre}_n"] = fila["n"]
        with patch.object(server, "resolver_puuid", return_value="P" * 78), \
             patch.object(server, "run_query", return_value=filas):
            r = fn(days=90, rol="JUNGLE")
        self.assertTrue(r["prioridades"])
        for c in r["prioridades"]:
            self.assertIn("NO digas que viene mejorando ni empeorando",
                          c["como_reportar"])


if __name__ == "__main__":
    unittest.main()
