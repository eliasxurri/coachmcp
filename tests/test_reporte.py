"""
Cada métrica viaja con la instrucción de cómo puede reportarse.

La advertencia vive también en los docstrings, pero eso no alcanzó: en uso
real el asistente enunció como hecho una métrica clasificada como indicio.
Al redactar la respuesta tiene el número delante y la descripción de la
herramienta muy atrás, así que la restricción tiene que ir pegada al dato.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "mcp-server"))

with patch("boto3.client"):
    import server


def filas_de_ventanas(medias_recientes: dict, medias_previas: dict) -> list[dict]:
    """Arma lo que devolvería Athena para las dos ventanas de get_trends."""
    filas = []
    for ventana, medias in (("reciente", medias_recientes), ("previo", medias_previas)):
        fila = {"ventana": ventana, "n": "60", "victorias": "30"}
        for nombre, _, _ in server.METRICAS:
            fila[f"{nombre}_avg"] = str(medias.get(nombre, 5.0))
            fila[f"{nombre}_sd"] = "1.0"
            fila[f"{nombre}_n"] = "60"
        filas.append(fila)
    return filas


class ComoReportarTests(unittest.TestCase):
    def _tendencias(self, recientes, previas):
        fn = getattr(server.get_trends, "fn", None) or server.get_trends
        with patch.object(server, "resolver_puuid", return_value="P" * 78), \
             patch.object(server, "run_query",
                          return_value=filas_de_ventanas(recientes, previas)):
            return fn(days=30)

    def test_toda_metrica_trae_instruccion_de_reporte(self):
        r = self._tendencias({}, {})
        self.assertTrue(r["metricas"])
        for m in r["metricas"]:
            self.assertIn("como_reportar", m, f"falta en {m['metrica']}")
            self.assertTrue(m["como_reportar"].strip())

    def test_el_ruido_se_marca_como_no_reportable(self):
        """Sin diferencia entre ventanas, todo debe quedar en ruido."""
        r = self._tendencias({}, {})
        for m in r["metricas"]:
            if m["clasificacion"] == "ruido":
                self.assertIn("NO reportar", m["como_reportar"])

    def test_un_indicio_dice_explicitamente_que_no_se_afirme(self):
        """
        Es el caso que falló en uso real: un indicio enunciado como hecho.
        """
        # Una sola métrica movida: pasa el umbral simple, pero con el resto
        # planas la corrección la deja como indicio.
        r = self._tendencias({"deaths": 5.5}, {"deaths": 5.0})
        indicios = [m for m in r["metricas"] if m["clasificacion"] == "indicio"]
        self.assertTrue(indicios, "el escenario debía producir al menos un indicio")
        for m in indicios:
            self.assertIn("NO afirmar", m["como_reportar"])
            self.assertIn("pista", m["como_reportar"])

    def test_las_prioridades_avisan_que_no_son_una_tendencia(self):
        """
        La confusión concreta fue mezclar la comparación con pares (una foto)
        con una afirmación sobre el tiempo.
        """
        fn = getattr(server.get_coaching_priorities, "fn", None) or server.get_coaching_priorities
        agregados = [
            {"grupo": "jugador", "n": "50", "jugadores": "1"},
            {"grupo": "pares", "n": "300", "jugadores": "280"},
        ]
        for fila in agregados:
            for nombre, _, _, _ in server.METRICAS_PARES:
                fila[f"{nombre}_avg"] = "5.0" if fila["grupo"] == "jugador" else "5.1"
                fila[f"{nombre}_sd"] = "1.0"
                fila[f"{nombre}_n"] = fila["n"]
        with patch.object(server, "resolver_puuid", return_value="P" * 78), \
             patch.object(server, "run_query", return_value=agregados):
            r = fn(days=90, rol="JUNGLE")
        self.assertIn("no dice si algo viene mejorando", r["nota"])
        self.assertIn("get_trends", r["nota"])


if __name__ == "__main__":
    unittest.main()


class InstruccionesDelServidorTests(unittest.TestCase):
    """
    El handshake entrega estas instrucciones antes de cualquier llamada.
    Son el único lugar donde cabe decir qué NO puede hacer el servidor, que
    es lo que evita que el modelo invente explicaciones ante un hueco.
    """

    def test_el_servidor_declara_sus_limites_y_la_disciplina_de_reporte(self):
        texto = server.INSTRUCCIONES
        self.assertTrue(server.mcp.instructions)
        for esperado in ("No consulta la API de Riot en vivo",
                         "como_reportar",
                         "indicio",
                         "ranked solo/duo"):
            self.assertIn(esperado, texto, f"falta: {esperado}")

    def test_prohibe_explicar_huecos_inventando_como_funciona_la_api(self):
        """El error concreto: afirmar que el rango 'no viene en la API'."""
        self.assertIn("No expliques por qué falta suponiendo cómo funciona",
                      server.INSTRUCCIONES)
