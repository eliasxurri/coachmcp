"""
Medir el LP por partida es lo que separa un plan de ascenso de una
conjetura: con el mismo winrate, la meta pasa de alcanzable a imposible
según cuánto mueva cada partida.

Estas pruebas fijan la atribución —solo cuentan los intervalos con una
sola partida— y el tratamiento del LP doble por rol prioritario.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "mcp-server"))

with patch("boto3.client"), patch("boto3.resource"):
    import server


def punto(momento, lp, v, d, tier="MASTER"):
    return {"momento": momento, "lp": lp, "victorias": v, "derrotas": d, "tier": tier}


class ProgresoLPTests(unittest.TestCase):
    def _medir(self, puntos):
        fn = getattr(server.get_lp_progress, "fn", None) or server.get_lp_progress
        tabla = MagicMock()
        tabla.query.return_value = {"Items": puntos}
        with patch.object(server, "resolver_puuid", return_value="P" * 78), \
             patch.object(server.dynamodb, "Table", return_value=tabla):
            return fn(days=30)

    def test_sin_serie_no_estima_nada(self):
        r = self._medir([punto(1, 311, 200, 169)])
        self.assertFalse(r["medible"])
        self.assertIn("NO estimes", r["nota"])

    def test_atribuye_victorias_y_derrotas_limpias(self):
        r = self._medir([
            punto(1, 300, 200, 169),
            punto(2, 318, 201, 169),   # +18 por una victoria
            punto(3, 303, 201, 170),   # -15 por una derrota
            punto(4, 321, 202, 170),   # +18 por otra victoria
        ])
        self.assertTrue(r["medible"])
        self.assertEqual(r["lp_por_victoria"], 18)
        self.assertEqual(r["lp_por_derrota"], 15)
        self.assertEqual(r["victorias_medidas"], 2)

    def test_descarta_los_intervalos_con_varias_partidas(self):
        """Dos partidas en un intervalo no permiten atribuir el cambio."""
        r = self._medir([
            punto(1, 300, 200, 169),
            punto(2, 305, 201, 170),   # una victoria y una derrota juntas
        ])
        self.assertEqual(r["intervalos_ambiguos"], 1)
        self.assertEqual(r["victorias_medidas"], 0)

    def test_ignora_el_salto_por_cambio_de_tier(self):
        """Ascender reinicia los LP: ese salto no mide una partida."""
        r = self._medir([
            punto(1, 95, 200, 169, tier="DIAMOND"),
            punto(2, 12, 201, 169, tier="MASTER"),
        ])
        self.assertEqual(r["victorias_medidas"], 0)

    def test_separa_las_victorias_con_lp_doble(self):
        """
        El LP doble por rol prioritario no representa una partida típica:
        promediarlo haría parecer que se sube más rápido de lo real.
        """
        r = self._medir([
            punto(1, 300, 200, 169),
            punto(2, 318, 201, 169),   # +18 normal
            punto(3, 336, 202, 169),   # +18 normal
            punto(4, 372, 203, 169),   # +36, compatible con bonificación
        ])
        self.assertEqual(r["lp_por_victoria"], 18)   # la mediana no se mueve
        self.assertEqual(r["victorias_bonificadas"]["cantidad"], 1)
        self.assertEqual(r["victorias_bonificadas"]["valores"], [36])


if __name__ == "__main__":
    unittest.main()
