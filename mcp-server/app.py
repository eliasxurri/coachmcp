"""
Envoltura HTTP del servidor MCP, para conectarlo a Claude.ai.

El servidor por stdio (server.py) exige venv, credenciales de AWS y editar
un archivo de configuración: sirve para desarrollar, no para que lo use un
jugador. Claude.ai acepta conectores remotos en todos sus planes pegando una
URL, y eso es lo que este módulo expone.

Las 7 herramientas no cambian: cambia el transporte. `streamable_http_app`
sirve el mismo objeto `mcp` sobre HTTP, en modo stateless para que cada POST
sea independiente y quepa en Lambda sin sesiones ni SSE que mantener.

Identidad: la URL de cada usuario lleva su token (/u/<token>/mcp). El
middleware lo cambia por su PUUID y lo deja en el contextvar que
`resolver_puuid` consulta, de modo que las herramientas solo pueden ver los
datos de quien hizo la petición.
"""

import asyncio
import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

import server
from server import mcp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lol-coach-http")

TABLA_USUARIOS = os.environ.get("USERS_TABLE", "lol-pipeline-usuarios")
RUTA_MCP = "/mcp"

dynamodb = boto3.resource("dynamodb")


def buscar_usuario(token: str) -> dict | None:
    """Cambia un token por el usuario dueño de esos datos."""
    try:
        item = dynamodb.Table(TABLA_USUARIOS).get_item(Key={"token": token}).get("Item")
    except ClientError:
        logger.exception("No se pudo leer la tabla de usuarios")
        return None
    return item if item and item.get("puuid") else None


def registrar_uso(token: str) -> None:
    """
    Marca el último uso. Es la señal que decide el beta: cuántos usuarios
    vuelven una segunda semana sin que se lo pidan.
    """
    try:
        dynamodb.Table(TABLA_USUARIOS).update_item(
            Key={"token": token},
            UpdateExpression="SET ultimo_uso = :ahora ADD llamadas :una",
            ExpressionAttributeValues={":ahora": int(time.time()), ":una": 1},
        )
    except ClientError:
        # Perder una marca de uso no justifica romperle la sesión a nadie.
        logger.warning("No se pudo registrar el uso del token", exc_info=True)


@mcp.custom_route("/salud", ["GET"])
async def salud(request):
    """Verificación sin autenticar, para comprobar que la Lambda responde."""
    return JSONResponse({"estado": "ok"})


# El SDK valida el header Host contra una lista blanca para frenar ataques
# de DNS rebinding. El valor por defecto asume localhost, así que detrás de
# API Gateway hay que declarar el dominio público o toda petición real
# responde 421. Se toma de una variable de entorno para no fijar en el
# código un identificador que Terraform genera.
HOSTS_PERMITIDOS = [h.strip() for h in os.environ.get("ALLOWED_HOSTS", "").split(",") if h.strip()]

seguridad = TransportSecuritySettings(
    enable_dns_rebinding_protection=bool(HOSTS_PERMITIDOS),
    allowed_hosts=HOSTS_PERMITIDOS,
    allowed_origins=[f"https://{h}" for h in HOSTS_PERMITIDOS],
)

aplicacion_mcp = mcp.streamable_http_app(
    streamable_http_path=RUTA_MCP,
    stateless_http=True,
    json_response=True,
    transport_security=seguridad,
)


async def app(scope, receive, send):
    """
    ASGI que resuelve el token de la ruta antes de delegar en el MCP.

    Se escribe a mano en vez de usar un Mount de Starlette porque hay que
    reescribir el path (/u/<token>/mcp -> /mcp) y fijar el contextvar dentro
    del alcance de la petición.
    """
    if scope["type"] != "http":
        await aplicacion_mcp(scope, receive, send)
        return

    ruta = scope.get("path", "")

    if ruta == "/salud":
        await aplicacion_mcp(scope, receive, send)
        return

    if not ruta.startswith("/u/"):
        await _rechazar(send, 404, "Ruta desconocida. Usá la URL que te entregaron.")
        return

    resto = ruta[len("/u/"):]
    token, _, cola = resto.partition("/")
    if not token or f"/{cola}" != RUTA_MCP:
        await _rechazar(send, 404, "Ruta desconocida. Usá la URL que te entregaron.")
        return

    usuario = buscar_usuario(token)
    if usuario is None:
        # Mismo mensaje para token inexistente y malformado: distinguirlos
        # ayudaría a adivinar tokens válidos.
        await _rechazar(send, 401, "Token inválido.")
        return

    registrar_uso(token)
    logger.info(
        json.dumps({
            "evento": "peticion_mcp",
            "token": token[:8],
            "riot_id": usuario.get("riot_id"),
        })
    )

    scope = dict(scope, path=RUTA_MCP, raw_path=RUTA_MCP.encode())
    testigo = server.puuid_de_sesion.set(usuario["puuid"])
    try:
        await aplicacion_mcp(scope, receive, send)
    finally:
        server.puuid_de_sesion.reset(testigo)


async def _rechazar(send, codigo: int, mensaje: str) -> None:
    cuerpo = json.dumps({"error": mensaje}).encode()
    await send({
        "type": "http.response.start",
        "status": codigo,
        "headers": [(b"content-type", b"application/json")],
    })
    await send({"type": "http.response.body", "body": cuerpo})


def iniciar_ciclo_de_vida() -> None:
    """
    Arranca el ciclo de vida ASGI una vez por contenedor.

    El gestor de streamable_http crea su task group en el arranque del ciclo
    de vida, y ese grupo queda atado al event loop que lo creó. Mangum, en
    cambio, entra y sale del ciclo en cada invocación: la primera llamada
    funciona, la segunda falla al reiniciar algo que ya estaba iniciado.

    Como Mangum reutiliza `asyncio.get_event_loop()` mientras el contenedor
    sigue caliente, alcanza con arrancar el ciclo acá —una sola vez, sin
    cerrarlo nunca— y decirle a Mangum que no lo administre. En un arranque
    en frío todo esto se repite desde cero, que es justo lo que se quiere.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    cola: asyncio.Queue = asyncio.Queue()
    cola.put_nowait({"type": "lifespan.startup"})
    listo = loop.create_future()

    async def enviar(mensaje):
        if listo.done():
            return
        if mensaje["type"] == "lifespan.startup.complete":
            listo.set_result(True)
        elif mensaje["type"] == "lifespan.startup.failed":
            listo.set_exception(RuntimeError(mensaje.get("message", "arranque fallido")))

    scope = {"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}
    # La tarea queda viva a propósito: mientras corra, el task group existe.
    loop.create_task(aplicacion_mcp(scope, cola.get, enviar))
    loop.run_until_complete(listo)


# Punto de entrada de Lambda. Mangum traduce el evento de API Gateway al
# protocolo ASGI que espera Starlette.
try:
    from mangum import Mangum

    iniciar_ciclo_de_vida()
    handler = Mangum(app, lifespan="off")
except ImportError:  # en local alcanza con uvicorn, que ya lo administra
    handler = None
