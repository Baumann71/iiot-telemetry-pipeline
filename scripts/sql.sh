#!/usr/bin/env bash
# Consulta a TimescaleDB.
#   scripts/sql.sh                          -> abre psql interactivo
#   scripts/sql.sh "SELECT count(*) FROM lecturas_v1;"

source "$(dirname "$0")/_comun.sh"

if [ $# -eq 0 ]; then
  # -t solo si hay una terminal real (si no, docker se queja de "not a TTY")
  if [ -t 0 ] && [ -t 1 ]; then
    docker exec -it timescaledb psql -U "$PG_USER" -d "$PG_DB"
  else
    docker exec -i timescaledb psql -U "$PG_USER" -d "$PG_DB"
  fi
else
  docker exec timescaledb psql -U "$PG_USER" -d "$PG_DB" -c "$1"
fi
