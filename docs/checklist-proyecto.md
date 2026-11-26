# Checklist Proyecto 1: Log distribuido y replicado con Apache Kafka

Entrega: 26 de noviembre de 2026. Informe de hasta 10 páginas y repo con README (Docker).
Los extras se pueden hacer después de entregar; si falta tiempo, se sacan primero.

## Núcleo (lo que aprueba la consigna)

### Base ya hecha
- [x] Simulador SCADA en Python (presión, temperatura, caudal)
- [x] EMQX configurado

### Infraestructura
- [x] `docker-compose` con 3 brokers de Kafka en KRaft (RF 3, min ISR 2, heap chico) que corra en una sola máquina
- [x] Crear topics con particiones y retención acotada (`telemetria.raw` con 6 particiones; servicio `kafka-init` del compose, 7 días)
- [x] Base de datos (TimescaleDB) en el compose, con hypertable (`lecturas_v1` y `lecturas_v2`, en `db/init/01-schema.sql`)
- [x] `.env.example` y secretos fuera del repo

### Flujo de datos
- [x] Extender el simulador a N pozos con 3 sensores cada uno (`N_POZOS`, topic `scada/<pozo>/<sensor>`)
- [x] Puente MQTT→Kafka con el conector nativo de EMQX, key `pozo_id` y `acks=all` (config en `emqx/base.hocon`; el productor de EMQX no es idempotente: en un failover duplicó 12 mensajes en `telemetria.raw`, que el upsert de `v1` absorbió)
- [x] Grupo `v1` en Python: lote cada 60 s (`LOTE_S`, bajado de 5 min para la exposición), validación, commit manual del offset después de escribir, inserción idempotente (pozo, sensor, timestamp) (`consumidores/consumidor-timescale.py`; se corre con `uv run`, sin dockerizar)
- [x] Segundo grupo independiente `alertas`: umbrales y dato faltante; en el núcleo solo imprime o registra (`consumidores/alertas.py`; se corre con `uv run`, sin dockerizar)

### Demos de falla (lo que se evalúa)
- [ ] Un broker caído: failover del líder, el pipeline sigue y no se pierden datos (conteo de publicados contra consumidos)
- [ ] Dos brokers caídos: las escrituras se detienen; verificar cómo se manifiesta (timeout o `NotEnoughReplicas`) y explicar por qué (quórum, ISR, min ISR, consistencia contra disponibilidad)
- [ ] Consumidor muerto a mitad de lote y reiniciado: sin duplicados ni huecos
- [ ] Script para provocar y revertir cada falla, de modo que cualquiera pueda reproducirlas (parcial: brokers con `scripts-expo/broker.sh caer|levantar|estado N`; falta el consumidor muerto a mitad de lote)

#### Guion de la demo de brokers (en `comandos-expo.md`)
1. Reset desde 0 (`down -v` de ambos compose y `up -d`, primero Kafka) y gateway corriendo unos minutos antes, para que haya datos
2. Cae 1 broker: `broker.sh estado` muestra el ISR en 2 y los líderes movidos; Grafana sigue actualizando
3. Cae el 2.º: sin quórum KRaft y sin min ISR; el gateway sigue publicando (EMQX da el PUBACK sin esperar a Kafka) y en el dashboard de EMQX crece `queuing` de la action `kafka_local`. No reiniciar EMQX en este paso: el buffer está en RAM
4. Levantar los 2: `queuing` vuelve a 0, Grafana recupera el hueco y `control.sh` muestra que no hubo pérdida
5. Leader skew: tras levantar, los líderes quedan en el broker que sobrevivió. El rebalanceo automático corre cada `leader.imbalance.check.interval.seconds` (300 s por defecto), así que hay que bajar el intervalo en el compose o forzarlo con `kafka-leader-election.sh --election-type preferred --all-topic-partitions`
6. Replay a `lecturas_v2` (ver "Muy recomendable")

- [ ] Ensayar el guion completo desde 0 un día antes
- [ ] Decidir cómo mostrar el rebalanceo de líderes (bajar el intervalo en el compose o forzarlo) y agregarlo a `comandos-expo.md`

#### Limitación conocida: buffer de la action de EMQX
Con Kafka caído, los mensajes quedan en el buffer de la action `kafka_local`, no en la cola de paho del gateway. Con `buffer.mode = memory` y los valores por defecto (`per_partition_limit = 256MB`, `memory_overload_protection = true`), el tope es de unos 1,5 GB para las 6 particiones. Al llenarse, EMQX descarta los mensajes más viejos (`dropped.queue_full`), y si se reinicia pierde todo lo encolado. Para caídas largas: `buffer.mode = disk` sobre el volumen `emqx_data`.
- [ ] Microbatch de `v1` acotado también por tamaño, no solo por tiempo (`LOTE_S`): en la puesta al día tras una caída, el lote de 60 s puede juntar todo el backlog en RAM y en un solo `executemany`

### Entregables
- [ ] README con instrucciones claras de ejecución con `docker compose up`
- [ ] Informe (≤10 páginas): Introducción, Relevancia en Sistemas Distribuidos, Desarrollo, Bibliografía

## Muy recomendable (para la exposición)

- [ ] Grupo `v2`: replay desde el offset 0 con lógica corregida (por ejemplo, unidades), tabla nueva, comparación contra `v1` (parcial: el replay ya funciona con `GROUP_ID=v2 TABLA=lecturas_v2` y `auto.offset.reset=earliest`; falta la lógica corregida y la comparación contra `v1`)
- [ ] Apuntar Grafana a la tabla nueva, apagar `v1` y borrar su grupo (`kafka-consumer-groups --delete`) (parcial: la variable `tabla` de Grafana ya elige entre `lecturas_v1` y `lecturas_v2`; falta apagar `v1` y borrar su grupo)
- [ ] Demo de desacople: apagar la base, ver subir el lag de `v1` y que se pone al día al volver; las alertas siguen
- [ ] Demo de descartes: el simulador inyecta mensajes inválidos y se ven en `telemetria.descartes` (`v2` no publica descartes)
- [x] `kafka-consumer-groups --describe` mostrando `v1` y `alertas` con offsets y lag distintos sobre el mismo topic (`scripts-expo/grupos.sh`)
- [x] Marcado de calidad de lecturas (buena, sospechosa, mala) con estado mínimo entre lotes, y alertas que ignoran las malas (`consumidores/calidad.py`; demo en el README)
- [x] Dashboard de Grafana con las tres variables y panel de "último dato recibido por sensor" (`grafana/dashboard/telemetria.json`, provisionado)
- [ ] Para la exposición: VM con Docker y Tailscale, brokers anunciando la IP de Tailscale en `advertised.listeners`
- [ ] Para la exposición: video grabado del failover como respaldo

## Extras (después, si hay tiempo)

- [x] Aviso por Telegram desde el grupo `alertas` (`consumidores/notificador.py`, opcional con `TELEGRAM_TOKEN`/`TELEGRAM_CHAT_ID`)
- [ ] Bot de Telegram: el operador responde "recibido" y se publica en `alertas.acks` (con quién y cuándo)
- [ ] Productor de clima (Open-Meteo) hacia `clima.pozos`, con cache y reintentos, y misma retención que `telemetria.raw`
- [ ] Enriquecimiento del lote con el clima de `clima.pozos`
- [ ] Pico de carga: subir la frecuencia de publicación, ver crecer el lag y levantar una segunda instancia del consumidor
- [ ] Detección simple de anomalías con z-score
- [ ] Pulido general de Grafana
- [ ] Continuous aggregate de TimescaleDB (promedio por hora): solo rendimiento en rangos largos, queda como trabajo futuro

## Fuera de alcance

Eventos manuales desde el celular, archivado a Parquet, topic `pozos.estado`, topic `alertas.generadas`, variante con Kafka Connect, dbt y Airflow.