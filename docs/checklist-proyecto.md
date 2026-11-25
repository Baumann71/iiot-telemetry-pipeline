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
- [ ] Script para provocar y revertir cada falla, de modo que cualquiera pueda reproducirlas

### Entregables
- [ ] README con instrucciones claras de ejecución con `docker compose up`
- [ ] Informe (≤10 páginas): Introducción, Relevancia en Sistemas Distribuidos, Desarrollo, Bibliografía

## Muy recomendable (para la exposición)

- [ ] Grupo `v2`: replay desde el offset 0 con lógica corregida (por ejemplo, unidades), tabla nueva, comparación contra `v1`
- [ ] Apuntar Grafana a la tabla nueva, apagar `v1` y borrar su grupo (`kafka-consumer-groups --delete`)
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