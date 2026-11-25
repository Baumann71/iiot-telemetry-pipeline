#!/usr/bin/env bash
# Offsets y lag de los consumer groups sobre el mismo topic.
#   scripts-expo/grupos.sh                  -> v1 y alertas
#   scripts-expo/grupos.sh v1 v2
#
# Muestra que cada grupo lleva su propia posición en el log: v1 avanza de a
# lotes (el lag sube y baja), alertas va al día.

source "$(dirname "$0")/_comun.sh"

GRUPOS="${*:-v1 alertas}"
EXISTENTES=$(kafka_tool kafka-consumer-groups.sh --list | tr -d '\r')

for g in $GRUPOS; do
  echo "== grupo $g"
  if ! grep -qx "$g" <<< "$EXISTENTES"; then
    echo "   (no existe todavía: nunca se conectó un consumidor con este group.id)"
    continue
  fi
  # solo las columnas que importan para la demo
  kafka_tool kafka-consumer-groups.sh --describe --group "$g" 2>/dev/null \
    | awk 'NF && $1 != "Consumer" {printf "   %-10s %-16s %-10s %-15s %-15s %s\n", $1, $2, $3, $4, $5, $6}'
done
