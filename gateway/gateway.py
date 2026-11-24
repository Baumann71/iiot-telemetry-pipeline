"""
este gateway seria el cliente/maestro, que tiene 2 funcionalidades
1. Pedirle los datos al servidor/esclavo, en este caso N "plc" (simulados), uno por pozo (unit id 1..N)
2. Publicar los datos al broker mqtt, un mensaje por sensor en scada/<pozo_id>/<sensor>
"""
import asyncio
import os
from publisher import MQTTPub
import logging
from  async_client import leer_planta, crear_cliente
from datetime import datetime, timezone
import argparse

#config del ckiente modbus tcp
#N_POZOS tiene que ser el mismo que en server.py: si se pide un unit id que no existe, el simulador de pymodbus falla
N_POZOS = int(os.getenv("N_POZOS", "6"))   #se consultan los unit ids 1..N_POZOS
HOST_ESCLAVO= os.getenv("MODBUS_HOST", "127.0.0.1")
PORT_ESCLAVO= int(os.getenv("MODBUS_PORT", "5020"))

logging.getLogger("pymodbus").setLevel(logging.ERROR)
BROKER_PORT= int(os.getenv("BROKER_PORT", "1883"))
BROKER_IP = os.getenv("BROKER_IP", "127.0.0.1")
CLIENT_ID= "gw-scada-01" #un solo gateway consulta a todos los pozos y publica por ellos
QOS = 1

#ahora hay N_POZOS * 3 mensajes por ciclo, la cola de paho tiene que alcanzar para varios ciclos sin conexion
#con 6 pozos y 1000 mensajes son ~55 ciclos (~4.5 min a 5 s), lo que exceda eso se descarta y queda en el log
MAX_COLA = int(os.getenv("MAX_COLA", "1000"))

DELAY_PER_PUB = float(os.getenv("INTERVALO_S", "5")) #cada cuantos seg se lee a todos los pozos y se publica

#nombre de cada sensor en el topic y en el payload, en el orden que devuelve leer_planta
SENSORES = ("presion", "temperatura", "caudal")

def pozo_id(device_id):
    #unit id 1 -> "pozo-01", es la key con la que se particiona en kafka
    return f"pozo-{device_id:02d}"



logging.basicConfig(
    level=logging.DEBUG,   
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("pymodbus").setLevel(logging.ERROR)
logger = logging.getLogger("gateway")



# el pubnlish lo llama el hilo main, el cual arma el paquete  y le pasa la ejecucion
# al hilo de paho (loop start) el cual luego devuelvhace el return del metodo publish

async def main():
    #primero creamos el cliente 
    clienteModbus= crear_cliente(HOST_ESCLAVO, PORT_ESCLAVO)
    #ahora creamos el Pub
    pub = MQTTPub(BROKER_IP,BROKER_PORT, CLIENT_ID, max_quued_messages=MAX_COLA) #max_quued_messages es el tamaño de la cola de mensajes pendientes, muy importante entender ese concepto de paho, es distinto a la mesage queue del broker

    #contador por (pozo, sensor), va en el payload como "seq"
    #sirve para las demos: comparando seq publicados contra filas en la base se detectan huecos o duplicados
    #vive en memoria, x ende vuelve a 0 si se reinicia el gateway
    seq = {(i, s): 0 for i in range(1, N_POZOS + 1) for s in SENSORES}

    try:
        #conectamos al cliente
        conectado= await clienteModbus.connect()
        if not conectado:
            logger.error("No se pudo conectar con el PLC, terminando ejecucción....")
            return #antes de retornar se ejecuta finally, x ende se cerraria el cliente tambien
        
        logger.info("Cliente Modbus conectado")


        #conectamos el pub al broker y tambien inicamos el loop_start (hilo aparte de paho para que no bloquee este)
        pub.start()
        loop = asyncio.get_running_loop()
        while True:
            inicio = loop.time()

            for device_id in range(1, N_POZOS + 1):
                pid = pozo_id(device_id)
                try:
                    valores = await leer_planta(clienteModbus, device_id)
                    logger.info("%s presion=%.2f bar temperatura=%.1f C caudal=%.1f m3/d", pid, *valores)
                except Exception as e:
                    #si falla un pozo se sigue con los demas, no se saltea todo el ciclo
                    logger.exception("%s fallo en lectura %s", pid, e)
                    continue

                #un solo timestamp por lectura del pozo: los 3 sensores se leyeron en el mismo pedido modbus
                ts = datetime.now(timezone.utc).isoformat() #pasa el datime a string porque el json.dumps no puede convertir datetime a str

                for sensor, valor in zip(SENSORES, valores):
                    seq[(device_id, sensor)] += 1
                    #payload autocontenido: el consumidor no necesita parsear el topic ni la key
                    datos = {
                        "pozo_id": pid,
                        "sensor": sensor,
                        "valor": valor,
                        "ts": ts,
                        "seq": seq[(device_id, sensor)],
                    }
                    topic = f"scada/{pid}/{sensor}"

                    try:
                        publicado, mid =pub.publish(datos, topic, qos=QOS)
                        if publicado:
                            logger.debug("Mensaje encolado en paho (cola de msj pendientes, pendiente de confrimacion), topic = %s, MID = %s", topic, mid)
                        else:
                            logger.warning("Mensaje descartado, no entro a la cola, topic = %s, seq = %s, MID = %s", topic, datos["seq"], mid)
                    except Exception as e:
                        logger.exception("Fallo al enviar: %s",e)

            #se descuenta lo que tardo el ciclo, asi se lee cada DELAY_PER_PUB seg y no cada DELAY_PER_PUB + tiempo de lectura
            await asyncio.sleep(max(0.0, DELAY_PER_PUB - (loop.time() - inicio)))
    except OSError as e:
        logger.exception("Fallo al ejecutar start() en el pub: %s",e)
                
    finally:
        clienteModbus.close()
        logger.info("Cliente Modbus cerrado")
        pub.stop()
        logger.info("Pub cerrado")
    
        
        
if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--broker_ip',
            type= str,
            help="IP del broker para la conexión"
        )
        
        parser.add_argument(
            '--broker_port', 
            type= int,
            help="PORT del broker para la conexión"
        )

        # Procesamos los argumentos de la terminal
        args = parser.parse_args()
        
        if args.broker_ip and args.broker_port:
            BROKER_IP = args.broker_ip
            BROKER_PORT = args.broker_port
        
        logger.debug("(Client Modbus TCP) valores de variables de entorno: N_POZOS = %s, HOST_ESCLAVO = %s, PORT_ESCLAVO = %s", N_POZOS, HOST_ESCLAVO, PORT_ESCLAVO)
        logger.debug("(BROKER) valores de variables de entorno: BROKER_IP = %s, BROKER_PORT = %s, CLIENT_ID = %s, QOS = %s, DELAY_PER_PUB = %s, MAX_COLA = %s", BROKER_IP, BROKER_PORT, CLIENT_ID, QOS, DELAY_PER_PUB, MAX_COLA)
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Termino la ejecucion del programa")
        pass
