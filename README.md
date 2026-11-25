# iiot-telemetry-pipeline

### Primera Parte del Proyecyto - Gateway Modbus → MQTT (simulación de planta)

Primera etapa del pipeline de telemetría industrial. Un servidor Modbus TCP (pymodbus) simula un PLC con nivel de tanque, temperatura y estado de bomba. Un gateway asíncrono (asyncio) lo consulta periódicamente, arma un mensaje JSON con timestamp en UTC y lo publica con QoS 1 a un broker EMQX (Docker), usando paho-mqtt.

La publicación está encapsulada en una clase propia que corre el cliente en un hilo aparte, no bloquea la lectura del PLC, registra las confirmaciones del broker y maneja los errores de lectura, de serialización y de cola llena.

Probé un corte de red del broker: los mensajes que entran en la cola de paho se entregan al reconectar, el resto se descarta y queda registrado.

Limitaciones: la cola de paho vive en memoria, así que se pierde si el proceso se cae. En un caso de aplicacion donde es fundamental no perder datos se implementaria un buzon persistente, por ejemplo con sqlite

El servidor permite ingresar la cantidad de devices a simular

### Infraestructura

* decidi separar el compose de kafka respecto al resto de la arquitectura, ya que el broker kafka va a ser implementado en una VM

### Puente MQTT → Kafka (conector nativo de EMQX)

El gateway publica un mensaje por sensor en `scada/<pozo_id>/<sensor>`. Una regla de EMQX toma `scada/#`, saca `pozo_id` del topic y una action lo publica en el topic de Kafka `telemetria.raw` con key `pozo_id` y `acks=all` (`all_isr`).

La configuración (connector, action y regla) está versionada en [emqx/base.hocon](emqx/base.hocon) y se monta en el contenedor, así que en un clon limpio el puente aparece solo, sin tocar el dashboard. Para apuntar a otros brokers (por ejemplo, la VM de la exposición), definir `KAFKA_BOOTSTRAP_HOSTS` en el `.env`.

Para verificarlo:

```bash
docker compose -f docker-compose.kafka.yml up -d
docker compose up -d emqx
uv run gateway/server.py      # en otra terminal: uv run gateway/gateway.py

# cada pozo tiene que caer siempre en la misma partición
docker exec -it kafka-1 bash -c "KAFKA_HEAP_OPTS=-Xmx64m /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic telemetria.raw --from-beginning \
  --property print.key=true --property print.partition=true"
```

También se ve en Kafka UI (http://localhost:8090, topic `telemetria.raw`) y en las métricas de la regla en el dashboard de EMQX (http://localhost:18083).

Detalle del particionado: la partición la elige EMQX, no Kafka, con `erlang:phash2(key) rem particiones` (no el murmur2 de los clientes de Kafka). Con 6 pozos, `pozo-02` a `pozo-05` caen en la partición 2 y `pozo-01` y `pozo-06` en la 5. Es una colisión del hash, no un error: el orden por pozo se mantiene igual.

### Consumidor
* para insertar el microbatch usamos ON CONFLICT DO NOTHING, esta es nuestra garantia para que el pipeline sea idempotente.

### Grupo `alertas`

[consumidores/alertas.py](consumidores/alertas.py) lee `telemetria.raw` en continuo, con su propio consumer group, y avisa en segundos. No usa la base: si TimescaleDB se cae, sigue funcionando.

- **Umbrales:** avisa solo cuando un sensor cambia de estado (`ALERTA ALTO`/`ALERTA BAJO` al salir del rango de operación, `NORMALIZADO` al volver), no en cada lectura.
- **Dato faltante:** si un sensor no reporta en `FALTANTE_S` segundos (20 por defecto), `SIN DATOS`; cuando vuelve, `RECUPERADO`.
- **Telegram (opcional):** con `TELEGRAM_TOKEN` y `TELEGRAM_CHAT_ID` en el `.env` también manda cada aviso a Telegram ([consumidores/notificador.py](consumidores/notificador.py)). Sin esas variables solo imprime.
- **Entrega al menos una vez:** el offset se confirma cada 5 s. Si el proceso se cae entre dos commits, al volver puede repetir alguna alerta.

Para reproducirlo:

```bash
EVENTOS_POR_HORA=60 uv run gateway/server.py   # ~1 anomalía por minuto (por defecto 0: sin anomalías)
uv run gateway/gateway.py
uv run consumidores/alertas.py
```

Cortando el gateway, a los 20 s aparece `SIN DATOS` para cada sensor, y `RECUPERADO` al volver a arrancarlo. Para disparar una alerta puntual a mano, ver [comandos-expo.md](comandos-expo.md). Con `alertas` corriendo y `v1` detenido, `scripts-expo/grupos.sh` muestra los dos grupos sobre el mismo topic con lags distintos.

### Calidad de lecturas

[consumidores/calidad.py](consumidores/calidad.py) califica cada lectura **válida** (las inválidas ya las descarta [validacion.py](consumidores/validacion.py)) comparándola con la anterior del mismo `(pozo, sensor)`:

| Calidad | Cuándo |
|---|---|
| `mala` | Valor **congelado** (6 lecturas iguales seguidas) o **salto imposible** (más de 40 bar/s, 25 °C/s o 80 m³/día/s) |
| `sospechosa` | Salto grande pero posible, instrumento saturado (pegado al borde de su rango) o lectura duplicada |
| `buena` | El resto |

- Los umbrales de salto están calibrados contra el simulador: un evento real (sobrepresión, etc.) salta como mucho ~17 bar/s, así que queda `sospechosa` y **se sigue avisando**. La calidad nunca tapa una alarma real.
- **`v1`/`v2`** guardan `calidad` y `calidad_motivo` en la tabla. El estado entre lotes (la lectura anterior de cada sensor) vive en memoria, se reconstruye al arrancar desde la misma tabla, y se adopta **recién después** de escribir el lote y confirmar el offset: si el consumidor muere a mitad de lote, el reproceso califica igual.
- **`alertas`** usa el mismo módulo con su propio estado en memoria (no espera a `v1` ni lee la base). Una lectura `mala` no dispara alertas: queda en el log como `IGNORADA`.

Para reproducirlo, con `v1` y `alertas` corriendo, inyectar lecturas en un pozo que el gateway no publica (`pozo-99`, así no se mezclan con las reales):

```bash
# pico: 40 -> 149 -> 41 con 1 s entre lecturas. 149 queda mala (109 bar/s) y alertas NO avisa "ALERTA ALTO"
uv run python -c "import json,time,datetime as d,paho.mqtt.publish as p
for v in (40,149,41): p.single('scada/pozo-99/presion', json.dumps({'pozo_id':'pozo-99','sensor':'presion','valor':v,'ts':d.datetime.now(d.timezone.utc).isoformat()}), qos=1, hostname='localhost'); time.sleep(1)"

# congelado: 7 veces el mismo valor. Desde la 6.ª lectura queda mala
uv run python -c "import json,time,datetime as d,paho.mqtt.publish as p
for _ in range(7): p.single('scada/pozo-99/temperatura', json.dumps({'pozo_id':'pozo-99','sensor':'temperatura','valor':55.5,'ts':d.datetime.now(d.timezone.utc).isoformat()}), qos=1, hostname='localhost'); time.sleep(1)"

# después del próximo lote de v1:
scripts-expo/sql.sh "SELECT ts, sensor, valor, calidad, calidad_motivo FROM lecturas_v1 WHERE pozo_id = 'pozo-99' ORDER BY ts;"
```

A los 20 s de la última inyección, `alertas` muestra `SIN DATOS` para `pozo-99`: es correcto, el pozo dejó de reportar.

### Grafana

`docker compose up -d grafana` y entrar a http://localhost:3000 (usuario `admin`, contraseña `GRAFANA_ADMIN_PASSWORD` del `.env`). El datasource ([grafana/provisioning/datasources/timescaledb.yml](grafana/provisioning/datasources/timescaledb.yml)) y el dashboard **Telemetria** ([grafana/dashboard/telemetria.json](grafana/dashboard/telemetria.json)) se cargan solos al arrancar.

- **Presión, temperatura y caudal:** una línea por pozo. Cada punto es el promedio de las lecturas dentro de un intervalo (mínimo 5 s, el período del gateway: en rangos cortos un punto es una lectura; en rangos largos Grafana agranda el intervalo). No incluye lecturas de calidad `mala`.
- **Último dato recibido por sensor:** cuánto hace que llegó la última fila de cada sensor a la base. Con `v1` escribiendo cada 60 s, hasta ~60 s es normal; si la base o `v1` se caen, se pone en rojo.
- **Variable `tabla`:** `lecturas_v1` o `lecturas_v2`. Es la forma de "apuntar Grafana a la tabla nueva" después del replay con `v2`.
- Grafana muestra lo que hay en la base, no lo que hay en Kafka: los gráficos avanzan de a un lote (60 s) aunque el dashboard refresque cada 30 s.

Para cambiar el dashboard: editarlo en la UI, Export → *Export as JSON* (con "Export for sharing externally" apagado) y reemplazar el JSON del repo; Grafana lo relee en 30 s. Desde la UI no se puede guardar porque está provisionado.
