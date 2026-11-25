#!/usr/bin/env bash
# Funciones compartidas por los scripts de la exposición. No se ejecuta solo:
# lo cargan los demás con "source".

set -euo pipefail

# Git Bash convierte rutas como /opt/kafka/... en rutas de Windows al pasarlas a
# docker exec; esto lo desactiva. En Linux y Mac no tiene efecto.
export MSYS_NO_PATHCONV=1

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Lee una variable del .env de la raíz (tolera espacios alrededor del "=" y comillas).
# Uso: env_var NOMBRE valor_por_defecto
env_var() {
  local v=""
  if [ -f "$RAIZ/.env" ]; then
    v=$(grep -E "^[[:space:]]*$1[[:space:]]*=" "$RAIZ/.env" | tail -1 | cut -d= -f2- \
        | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//; s/^"(.*)"$/\1/')
  fi
  echo "${v:-$2}"
}

PG_USER=$(env_var POSTGRES_USER telemetria)
PG_DB=$(env_var POSTGRES_DB telemetria)

BROKERS="kafka-1 kafka-2 kafka-3"
BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"

# Primer broker que esté corriendo: en las demos de falla alguno puede estar caído,
# así que las herramientas de Kafka se ejecutan dentro de uno que siga vivo.
broker_vivo() {
  local b
  for b in $BROKERS; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$b" 2>/dev/null)" = "true" ]; then
      echo "$b"; return 0
    fi
  done
  echo "No hay ningún broker de Kafka corriendo" >&2
  return 1
}

# Ejecuta una herramienta de /opt/kafka/bin dentro de un broker vivo.
# KAFKA_HEAP_OPTS=-Xmx64m: la JVM de la herramienta no compite con el broker
# dentro de su mem_limit (mismo truco que el healthcheck del compose).
# Uso: kafka_tool kafka-topics.sh --describe --topic telemetria.raw
kafka_tool() {
  local b
  b=$(broker_vivo)
  docker exec "$b" env KAFKA_HEAP_OPTS=-Xmx64m "/opt/kafka/bin/$1" --bootstrap-server "$BOOTSTRAP" "${@:2}"
}

# Valida un nombre de tabla antes de meterlo en una consulta.
validar_tabla() {
  if ! [[ "$1" =~ ^[a-z_][a-z0-9_]*$ ]]; then
    echo "Nombre de tabla inválido: $1" >&2
    exit 1
  fi
}
