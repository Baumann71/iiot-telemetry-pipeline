#!/usr/bin/env bash
# Provocar y revertir la caída de un broker.
#   scripts-expo/broker.sh caer 2        -> mata kafka-2 de golpe (docker kill, sin apagado ordenado)
#   scripts-expo/broker.sh levantar 2    -> lo vuelve a arrancar
#   scripts-expo/broker.sh estado        -> qué brokers corren, y líder e ISR de cada partición
#
# "caer" usa docker kill a propósito: simula una falla real. Con docker stop el
# broker haría un apagado controlado y movería el liderazgo antes de irse.

source "$(dirname "$0")/_comun.sh"

estado() {
  echo "== brokers"
  for b in $BROKERS; do
    printf "   %-8s %s\n" "$b" "$(docker inspect -f '{{.State.Status}}' "$b" 2>/dev/null || echo 'no existe')"
  done
  echo "== telemetria.raw (Leader = broker que atiende la partición, Isr = réplicas al día)"
  # 2>/dev/null: con un broker caído la herramienta avisa que no resuelve su nombre (es esperable)
  kafka_tool kafka-topics.sh --describe --topic telemetria.raw 2>/dev/null \
    | grep -E "Partition:" | awk '{printf "   %s %-3s %s %-3s %s %-7s %s %s\n", $3, $4, $5, $6, $7, $8, $9, $10}'
}

accion="${1:-}"
n="${2:-}"

case "$accion" in
  caer|levantar)
    if ! [[ "$n" =~ ^[1-3]$ ]]; then
      echo "Uso: $0 $accion <1|2|3>" >&2; exit 1
    fi
    if [ "$accion" = caer ]; then
      docker kill "kafka-$n" >/dev/null && echo "kafka-$n caído"
    else
      docker start "kafka-$n" >/dev/null && echo "kafka-$n arrancando (tarda unos segundos en volver al ISR)"
    fi
    ;;
  estado)
    estado
    ;;
  *)
    echo "Uso: $0 caer <n> | levantar <n> | estado" >&2; exit 1
    ;;
esac
