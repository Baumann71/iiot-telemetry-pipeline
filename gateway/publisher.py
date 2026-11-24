import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTProtocolVersion
import json
import logging

logger = logging.getLogger("publisher")

class MQTTPub():

    def __init__(self, ip_broker, port_broker, client_id, max_quued_messages=0):
        self._ip_broker = ip_broker
        self._port_broker = port_broker
        #config cliente
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id = client_id,
            protocol= MQTTProtocolVersion.MQTTv5,
            reconnect_on_failure=True, #este val es x defecto
        )
        self._client.max_queued_messages = max_quued_messages #0 es ilimitado
        
        #callbacks importantes para un pub
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_publish = self._on_publish
        
        
    def publish(self, dato, topic, qos=1):
        """
        En caso de ejecutar esto con un loop_start, se sabe que el hilo main que llama al loop start
        provoca que se cree un hilo aparte el cual se encarga de :
            1. enviar mensajes que otros hilos dejaron en la cola de mensajes pendientes de paho (x ej tras que esos otros hilos ejecutaran publish)
            2. leer del socker lo que manda el broker (confirmaciones, mensajes de suscripciones) e interpretar esos paquetes
            3. llamar a los callbacks (on_connect, on_publish, on_message, on_disconnect) cuando se debe
            4. mantener viva la conexion: enviar el Ping para el Keep Alive y detectar si el broker dejó de responder
            5. reconectar con espera creciente si es que el nos caimos 
            6. llevar el estado de QoS 1 y 2: reenviar lo que quedó sin confirmar tras una reconexión y completar los intercambios de confirmación 
        
        pero el metodo de publish de esta clase MQTTPub (no la de self._client.publish() !!! ) cuando lo invoca un otro hilo (no el del loop start) lo que ocurre es:
            1. ese otro hilo arma el paquete, lo pone en la cola interna de paho (la de mensajes pendientes) y avisa al hilo de red que hay algo
            2. el hilo de pao, que esta esperando sobre el socker, ve el aviso y escribe en el socket hacia el broker (se lo envia). Tambíen es el que
                lee las respuestas y ejecuta los callbcks
        por ende lo que retorne esta funcion es lo que recibe ese otro hilo como confirmacion de que su mensaje al menos lo capturo el hilo del loop start de paho
        ya sea que lo envió o que qeudo en la cola de mensajes pendientes de paho (eso seri aun True), o si no se pudo guardar en la cola (un false)
        
        Ahora el client.publish ese metodo de pago retorna un MQTTMessageInfo con un código rc, El True/False es el de tu clase MQTTPub, que traduce ese código. Los casos que vimos:
            rc = 0 (SUCCESS): el mensaje quedó en la cola, listo para que el hilo de red lo envíe.
            rc = 4 (NO_CONN): no hay conexión. Con QoS 1 o 2 igual queda en la cola y se reenvía al reconectar. Con QoS 0 se pierde.
            rc = 15 (QUEUE_SIZE): la cola está llena y el mensaje no se guardó.
            
        No dice que el mensaje se envió ni que el broker lo recibió: eso lo informa on_publish. Si los datos son inválidos, lanza una excepción.
        """
        
        if not topic:
            logger.warning("(publish) No declaraste topicos para publicar")
            return False, None #tecnicamente no sirve ponerle un valor, porque esta funcion la ejecuta paho no nosotros y paho no es que usa el valor retornado para algo

        try:            
            payload = json.dumps(dato)
        except (TypeError, ValueError) as e:
            return False, None

        try: 
            info_msg = self._client.publish(topic, payload, qos=qos) # este metodo devuelve un MQTTMessageInfo, 
        except (TypeError, ValueError):
            return False, None
        
        if info_msg.rc == mqtt.MQTT_ERR_QUEUE_SIZE:
            logger.error("(publish) cola de paho llena, mensaje descartado")
            return False, info_msg.mid
        
        if info_msg.rc == mqtt.MQTT_ERR_NO_CONN and qos == 0:
            logger.error("(publish) sin conexión, mensaje QoS 0 perdido")
            return False, info_msg.mid
        
        #not in err_success y err_no_conn porque el success asegura que o salio a la red o esta en la cola de pendientes del cliente
        #y el err no conn es para que cuando no hay conexion y la cola tiene lugar y el mesnaje es al QoS 1/2 entonces
        #lo deja en la cola del cliente y al reconectar los envia
        #pero con qos 0 si significa que se perdio con el err no conn por eso arriba esta ese condicional
        if info_msg.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
            logger.error(f"(publish) publish fallo, rc: {info_msg.rc}")
            return False, info_msg.mid
        
        return True, info_msg.mid
    
    def _on_connect(self, client, userdata, connect_flags, reason_code, properties):
        if reason_code.is_failure:
            logger.error("(_on_connect) Conexion rechazada: %s",reason_code)
            return 
        
        logger.info("(_on_connect) Conexion aceptada: %s", reason_code)
        logger.debug("(_on_connect) Propiedades: %s", properties)
        
        if connect_flags.session_present:
            logger.info("(_on_connect) Sesion persistente retomada")
        else:
            logger.info("(_on_connect) no se encontro sesion previa (o expiro)")
            
         
    
    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        logger.info("(_on_disconnect) Desconectado: %s", reason_code)
        logger.debug("(_on_disconnect) Propiedades: %s", properties)
    
    def _on_publish(self, client, userdata, mid, reason_code, properties):
        if reason_code.is_failure:
            logger.error("(_on_publish) el broker rechazo mid: %s, %s", mid, reason_code)
            return 

        logger.info("(_on_publish) Mensaje confirmado mid=%s", mid)
         
    
    def start(self, keepalive=60):
        try:
            self._client.connect(self._ip_broker, self._port_broker, keepalive= keepalive)
        except OSError:
            raise
        #si pudimos conectarnos entonces iniciamos el loop con el hilo aparte
        self._client.loop_start()
        
    def stop(self):
        self._client.disconnect()
        self._client.loop_stop()