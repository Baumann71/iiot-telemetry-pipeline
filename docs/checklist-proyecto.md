# Checklist Proyecto 1: Log distribuido y replicado con Apache Kafka

Entrega: 26 de diciembre de 2026. Informe de hasta 10 páginas y repo con README (Docker).
Los extras se pueden hacer después de entregar; si falta tiempo, se sacan primero.

## Núcleo (lo que aprueba la consigna)

### Base ya hecha
- [x] Simulador SCADA en Python (presión, temperatura, caudal)
- [x] EMQX configurado

### Infraestructura
- [X] `docker-compose` con 3 brokers de Kafka en KRaft (RF 3, min ISR 2, heap chico) que corra en una sola máquina
- [*] Crear topics con particiones y retención acotada (`telemetria.raw` con 6 particiones)
- [X] Base de datos (TimescaleDB) en el compose, con hypertable
- [X] `.env.example` y secretos fuera del repo

### Flujo de datos
- [x] Extender el simulador a N pozos con 3 sensores cada uno (`N_POZOS`, topic `scada/<pozo>/<sensor>`)
- [x] Puente MQTT→Kafka con el conector nativo de EMQX, key `pozo_id` y `acks=all` (config en `emqx/base.hocon`; falta verificar si el productor de EMQX soporta idempotencia)
- [x] Grupo `v1` en Python: lote cada 5 min, validación, commit manual del offset después de escribir, inserción idempotente (pozo, sensor, timestamp) (`consumidores/consumidor-timescale.py`; falta dockerizarlo)
- [ ] Segundo grupo independiente `alertas`: umbrales y dato faltante; en el núcleo solo imprime o registra

### Demos de falla (lo que se evalúa)
- [ ] Un broker caído: failover del líder, el pipeline sigue y no se pierden datos (conteo de publicados contra consumidos)
- [ ] Dos brokers caídos: las escrituras se detienen; verificar cómo se manifiesta (timeout o `NotEnoughReplicas`) y explicar por qué (quórum, ISR, min ISR, consistencia contra disponibilidad)
- [ ] Consumidor muerto a mitad de lote y reiniciado: sin duplicados ni huecos
- [ ] Script para provocar y revertir cada falla, de modo que cualquiera pueda reproducirlas

### Entregables
- [ ] Confirmar con la cátedra que Kafka es un tema aceptable
- [ ] README con instrucciones claras de ejecución con `docker compose up`
- [ ] Informe (≤10 páginas): Introducción, Relevancia en Sistemas Distribuidos, Desarrollo, Bibliografía
- [ ] Tag de la versión entregada (por ejemplo `entrega-2026`)

## Muy recomendable (para la exposición y para lucirse)

- [ ] Grupo `v2`: replay desde el offset 0 con lógica corregida (por ejemplo, unidades), tabla nueva, comparación contra `v1`
- [ ] Apuntar Grafana a la tabla nueva, apagar `v1` y borrar su grupo (`kafka-consumer-groups --delete`)
- [ ] Demo de desacople: apagar la base, ver subir el lag de `v1` y que se pone al día al volver; las alertas siguen
- [ ] Demo de descartes: el simulador inyecta mensajes inválidos y se ven en `telemetria.descartes` (`v2` no publica descartes)
- [ ] Medición de latencia (p50 y p95), probando también lotes de 1 minuto y cuánto tarda reprocesar N días
- [ ] `kafka-consumer-groups --describe` mostrando `v1` y `alertas` con offsets y lag distintos sobre el mismo topic
- [ ] Marcado de calidad de lecturas (buena, sospechosa, mala) con estado mínimo entre lotes, y alertas que ignoran las malas
- [ ] Continuous aggregate de TimescaleDB (promedio por hora)
- [ ] Dashboard de Grafana con las tres variables y panel de "último dato recibido por sensor"
- [ ] Para la exposición: VM con Docker y Tailscale, brokers anunciando la IP de Tailscale en `advertised.listeners`
- [ ] Para la exposición: video grabado del failover como respaldo

## Extras (después, si hay tiempo)

- [ ] Aviso por Telegram desde el grupo `alertas`
- [ ] Bot de Telegram: el operador responde "recibido" y se publica en `alertas.acks` (con quién y cuándo)
- [ ] Productor de clima (Open-Meteo) hacia `clima.pozos`, con cache y reintentos, y misma retención que `telemetria.raw`
- [ ] Enriquecimiento del lote con el clima de `clima.pozos`
- [ ] Pico de carga: subir la frecuencia de publicación, ver crecer el lag y levantar una segunda instancia del consumidor
- [ ] Detección simple de anomalías con z-score
- [ ] Pulido general de Grafana

## Fuera de alcance

Eventos manuales desde el celular, archivado a Parquet, topic `pozos.estado`, topic `alertas.generadas`, variante con Kafka Connect, dbt y Airflow.