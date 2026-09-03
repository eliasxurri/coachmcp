"""
El servidor remoto sirve a varios usuarios del beta desde un mismo data
lake, así que el aislamiento entre cuentas no es una validación más: es la
propiedad de la que depende que se pueda invitar a alguien.

Estas pruebas fijan que, con sesión activa, ninguna herramienta alcance
datos de otro jugador — ni pidiéndolos por Riot ID, ni por PUUID crudo.
"""

import asyncio
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "mcp-server"))

PUUID_A = "A" * 78
PUUID_B = "B" * 78


def cargar_modulos():
    os.environ.setdefault("USERS_TABLE", "tabla-de-prueba")
    with patch("boto3.client"), patch("boto3.resource"):
        import server
        import app
    return server, app


server, app_mod = cargar_modulos()


JUGADORES = [
    {"puuid": PUUID_A, "game_name": "ana", "riot_id": "ana#LAS", "partidas_ingeridas": 10},
    {"puuid": PUUID_B, "game_name": "beto", "riot_id": "beto#LAS", "partidas_ingeridas": 20},
]


class AislamientoTests(unittest.TestCase):
    def test_la_sesion_gana_sobre_el_riot_id_ajeno(self):
        """Pedir explícitamente a otro jugador devuelve los datos propios."""
        testigo = server.puuid_de_sesion.set(PUUID_A)
        try:
            self.assertEqual(server.resolver_puuid("beto#LAS"), PUUID_A)
            self.assertEqual(server.resolver_puuid("beto"), PUUID_A)
            self.assertEqual(server.resolver_puuid(None), PUUID_A)
        finally:
            server.puuid_de_sesion.reset(testigo)

    def test_la_sesion_gana_sobre_un_puuid_crudo(self):
        """Pasar el PUUID de otro tampoco escapa de la sesión."""
        testigo = server.puuid_de_sesion.set(PUUID_A)
        try:
            self.assertEqual(server.resolver_puuid(PUUID_B), PUUID_A)
        finally:
            server.puuid_de_sesion.reset(testigo)

    def test_list_players_solo_muestra_al_usuario(self):
        """No se filtran los Riot ID del resto del beta."""
        testigo = server.puuid_de_sesion.set(PUUID_A)
        try:
            with patch.object(server, "listar_jugadores", return_value=JUGADORES):
                tool = server.list_players
                fn = getattr(tool, "fn", None) or tool
                visibles = fn()
            self.assertEqual([j["puuid"] for j in visibles], [PUUID_A])
        finally:
            server.puuid_de_sesion.reset(testigo)

    def test_sin_sesion_el_modo_local_sigue_funcionando(self):
        """En stdio con un solo jugador, `player` se comporta como antes."""
        self.assertIsNone(server.puuid_de_sesion.get())
        with patch.object(server, "listar_jugadores", return_value=JUGADORES[:1]):
            self.assertEqual(server.resolver_puuid(None), PUUID_A)
        with patch.object(server, "listar_jugadores", return_value=JUGADORES):
            self.assertEqual(server.resolver_puuid("beto#LAS"), PUUID_B)


def llamar_asgi(ruta: str, metodo: str = "POST") -> tuple[int, bytes]:
    """Ejecuta una petición contra la app ASGI sin levantar un servidor."""
    respuesta = {}
    cuerpo = bytearray()

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(mensaje):
        if mensaje["type"] == "http.response.start":
            respuesta["status"] = mensaje["status"]
        elif mensaje["type"] == "http.response.body":
            cuerpo.extend(mensaje.get("body", b""))

    scope = {
        "type": "http", "method": metodo, "path": ruta, "raw_path": ruta.encode(),
        "headers": [], "query_string": b"", "scheme": "http",
        "server": ("test", 80), "client": ("test", 1), "asgi": {"version": "3.0"},
    }
    asyncio.run(app_mod.app(scope, receive, send))
    return respuesta.get("status"), bytes(cuerpo)


class RutasTests(unittest.TestCase):
    def test_token_desconocido_da_401(self):
        with patch.object(app_mod, "buscar_usuario", return_value=None):
            estado, cuerpo = llamar_asgi("/u/inventado/mcp")
        self.assertEqual(estado, 401)
        self.assertIn("inválido", json.loads(cuerpo)["error"])

    def test_rutas_fuera_del_patron_dan_404(self):
        for ruta in ["/", "/mcp", "/u//mcp", "/u/token", "/u/token/otra"]:
            with self.subTest(ruta=ruta):
                with patch.object(app_mod, "buscar_usuario", return_value=None):
                    estado, _ = llamar_asgi(ruta)
                self.assertEqual(estado, 404)

    def test_un_fallo_de_dynamodb_no_se_reporta_como_401(self):
        """
        Un 401 le dice a Claude.ai que el servidor pide iniciar sesión, y
        manda al usuario a reconfigurar la autenticación por un problema que
        no es suyo. Una caída del almacén tiene que dar 503.
        """
        with patch.object(app_mod, "buscar_usuario",
                          side_effect=app_mod.AlmacenNoDisponible("caida")):
            estado, cuerpo = llamar_asgi("/u/loquesea/mcp")
        self.assertEqual(estado, 503)
        self.assertNotIn("inválido", json.loads(cuerpo)["error"])

    def test_el_token_no_se_registra_entero_en_los_logs(self):
        """Un token en texto plano en CloudWatch es una credencial filtrada."""
        token = "t" * 40
        with patch.object(app_mod, "buscar_usuario",
                          return_value={"puuid": PUUID_A, "riot_id": "ana#LAS"}), \
             patch.object(app_mod, "registrar_uso"), \
             patch.object(app_mod, "aplicacion_mcp") as mcp_falso:
            async def noop(scope, receive, send):
                await send({"type": "http.response.start", "status": 200, "headers": []})
                await send({"type": "http.response.body", "body": b"{}"})
            mcp_falso.side_effect = noop
            with self.assertLogs("lol-coach-http", level="INFO") as registro:
                llamar_asgi(f"/u/{token}/mcp")
        self.assertNotIn(token, "\n".join(registro.output))


if __name__ == "__main__":
    unittest.main()
