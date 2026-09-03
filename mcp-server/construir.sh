#!/usr/bin/env bash
# Arma la carpeta que se despliega como Lambda.
#
# El archive_file de Terraform solo comprime un directorio: no instala
# dependencias. Este script deja en build/ las fuentes junto a sus
# dependencias, listas para empaquetar.
#
# boto3 y botocore se descartan a propósito: el runtime de Lambda ya los
# trae y son 28 MB de los 43 que ocuparía el paquete.
set -euo pipefail

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DESTINO="$AQUI/build"
PYTHON="${PYTHON:-python3}"

rm -rf "$DESTINO"
mkdir -p "$DESTINO"

"$PYTHON" -m pip install \
  --quiet --target "$DESTINO" \
  --requirement "$AQUI/requirements.txt"

# Provistos por el runtime de Lambda.
rm -rf "$DESTINO"/boto3 "$DESTINO"/botocore "$DESTINO"/boto3-* "$DESTINO"/botocore-*
# El bytecode sí sobra. Los *.dist-info NO se tocan: varias dependencias
# resuelven su propia versión con importlib.metadata al importarse, y sin
# esos directorios la función falla con "No package metadata was found".
find "$DESTINO" -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true

cp "$AQUI/server.py" "$AQUI/app.py" "$AQUI/estadistica.py" "$DESTINO/"

echo "build listo: $(du -sh "$DESTINO" | cut -f1)"
