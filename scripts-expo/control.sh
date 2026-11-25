#!/usr/bin/env bash
# Control de huecos y duplicados de una tabla de lecturas contra los offsets de Kafka.
#   scripts-expo/control.sh                 -> lecturas_v1
#   scripts-expo/control.sh lecturas_v2
#
# Por cada partición compara las filas en la base contra el rango de offsets que
# llegaron (max - min + 1). Si coinciden, no se saltó ningún mensaje. En la base
# no puede haber duplicados: la PK (pozo_id, ts, sensor) los rechaza.
#
# "sin_fila" > 0 NO significa necesariamente pérdida. Un offset queda sin fila si:
#   - el mensaje era inválido y se descartó (log del consumidor / telemetria.descartes), o
#   - el mensaje era un DUPLICADO en Kafka y el ON CONFLICT DO NOTHING lo ignoró.
#     Pasa en un failover: EMQX no recibe la confirmación, reenvía, y el log queda
#     con el mismo mensaje dos veces (su productor no es idempotente).
# Para ver cuáles son: scripts-expo/sql.sh "SELECT g FROM generate_series(<min>,<max>) g
#   WHERE g NOT IN (SELECT kafka_offset FROM lecturas_v1 WHERE kafka_part=<p>);"

source "$(dirname "$0")/_comun.sh"

TABLA="${1:-lecturas_v1}"
validar_tabla "$TABLA"

docker exec timescaledb psql -U "$PG_USER" -d "$PG_DB" -c "
SELECT kafka_part                                    AS particion,
       count(*)                                      AS filas,
       min(kafka_offset)                             AS offset_min,
       max(kafka_offset)                             AS offset_max,
       max(kafka_offset) - min(kafka_offset) + 1     AS esperadas,
       max(kafka_offset) - min(kafka_offset) + 1 - count(*) AS sin_fila,
       count(DISTINCT pozo_id)                       AS pozos,
       max(ingest_ts)                                AS ultima_insercion
FROM $TABLA
GROUP BY kafka_part
ORDER BY kafka_part;"
