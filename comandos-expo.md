

### para entrar a timescaldb:
* docker exec -it timescaledb psql -U root -d telemetria
    * \dt <- show tables>

### Scripts (`scripts-expo/`, correr desde la raíz del repo, en Git Bash/Linux/Mac)

Usuario y base de Postgres salen del `.env`. Las herramientas de Kafka se ejecutan dentro del primer broker que siga vivo, así que funcionan aunque haya brokers caídos.

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
### Migración de calidad (solo si la base se creó antes de `calidad_motivo`)
```bash
scripts-expo/sql.sh "ALTER TABLE lecturas_v1 ADD COLUMN IF NOT EXISTS calidad_motivo TEXT;"
```
`lecturas_v2` creada después de esto con `LIKE lecturas_v1 INCLUDING ALL` ya la trae.
