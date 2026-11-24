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
