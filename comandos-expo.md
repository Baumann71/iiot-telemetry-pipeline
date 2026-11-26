### Levantar compose
* docker compose -f docker-compose.kafka.yml up -d
* docker compose up -d

### Dar de baja un broker
* docker kill kafka-N <- esto simula una flla real, por eso no uso el stop
* docker start kafka-N <- lo levanta 
* 
### para entrar a timescaldb:
* docker exec -it timescaledb psql -U root -d telemetria
    * \dt <- show tables>

### Scripts (`scripts-expo/`()

| Comando | Qué muestra |
|---|---|
| `scripts-expo/sql.sh` | Abre psql. Con una consulta entre comillas, la corre y sale |
| `scripts-expo/control.sh [tabla]` | Por partición: filas en la base contra rango de offsets. `sin_fila` = offsets sin fila: inválidos descartados o duplicados de Kafka absorbidos por el upsert (no es pérdida). Por defecto `lecturas_v1` |
| `scripts-expo/grupos.sh [grupos...]` | Offset y lag de cada grupo sobre `telemetria.raw`. Por defecto `v1` y `alertas` |
| `scripts-expo/broker.sh estado` | Qué brokers corren, y líder e ISR de cada partición |
| `scripts-expo/broker.sh caer N` | `docker kill kafka-N`: falla abrupta, sin apagado ordenado |
| `scripts-expo/broker.sh levantar N` | Vuelve a arrancar `kafka-N` |


### Demo de alertas: 
EVENTOS_POR_HORA=60 uv run gateway/server.py    # ~1 evento por minuto en toda la planta

o sino un trigger directo:

```bash
uv run python -c "import json,datetime,paho.mqtt.publish as p; p.single('scada/pozo-03/presion', json.dumps({'pozo_id':'pozo-03','sensor':'presion','valor':152.1,'ts':datetime.datetime.now(datetime.timezone.utc).isoformat()}), qos=1, hostname='localhost')"
```


### Demo consumidor V2:
GROUP_ID=v2 TABLA=lecturas_v2 uv run consumidores/consumidor-timescale.py
