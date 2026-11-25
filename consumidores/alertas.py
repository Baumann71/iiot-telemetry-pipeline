#grupo de consumidores "alertas": lee telemetria.raw en continuo (no por lotes) y avisa en segundos
#   1. umbrales: un valor fuera de la operacion normal
#   2. dato faltante: un sensor que deja de reportar
#es independiente de la base: no usa TimescaleDB, si la base se cae alertas sigue avisando
#entrega "al menos una vez": si se cae entre dos commits, al volver puede repetir alguna alerta (preferible a perderla)
#el estado (umbrales y ultimo visto) vive en memoria: al reiniciar se pierde y se reevalua desde cero

from confluent_kafka import Consumer, KafkaError, KafkaException

import os
from dotenv import load_dotenv
import time
from datetime import datetime, timezone
from validacion import validar, MensajeInvalido
from calidad import calificar, MALA, SOSPECHOSA
from notificador import desde_entorno

load_dotenv()

GROUP_ID = "alertas"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092,localhost:29093,localhost:29094")
TOPIC = "telemetria.raw"

COMMIT_S = 5 #cada cuantos segundos se confirma el offset (haya o no alertas)

#si un sensor no reporta en este tiempo -> "SIN DATOS"; el gateway publica cada 5 s, 20 s son ~4 ciclos perdidos
FALTANTE_S = float(os.getenv("FALTANTE_S", "20"))

UMBRALES = {                     # operación normal (mín, máx)
    "presion":     (10.0, 100.0),
    "temperatura": (20.0, 100.0),
    "caudal":      (30.0, 300.0),
}
#ojo: los eventos esporadicos de gateway/server.py (EVENTOS) estan pensados para cruzar estos umbrales

config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "enable.auto.commit": False,
    "auto.offset.reset": "latest",   # no alertar por la historia vieja, solo importa cuando el consumidor no tiene un offset definido por ende arrancamos de la ultima alerta
}

#notificador de Telegram: sin TELEGRAM_TOKEN / TELEGRAM_CHAT_ID en el .env queda deshabilitado y solo se imprime
telegram = desde_entorno()


def emitir(texto):
    #unico lugar por donde sale un aviso: siempre al log, y a Telegram si esta habilitado
    print(texto)            # el log queda siempre, haya Telegram o no
    telegram.enviar(texto)  # no bloquea; sin token no hace nada


def hora():
    return datetime.now().strftime('%H:%M:%S')


def avisar(tipo, lectura, detalle=""):
    #latencia de punta a punta: desde que el gateway leyo el sensor hasta que alertas lo evalua
    latencia = (datetime.now(timezone.utc) - lectura.ts).total_seconds()
    #en local son milisegundos; con Kafka en la VM o con lag, segundos
    lat = f"{latencia * 1000:.0f} ms" if latencia < 1 else f"{latencia:.1f} s"
    emitir(f"{hora()} {tipo:<12} {lectura.pozo_id} {lectura.sensor} "
           f"= {lectura.valor:.2f} {detalle}  (llegó en {lat})")


def evaluar_umbral(lectura, estado_umbral, nota=""):
    #nota: aclaracion que se agrega al aviso (ej. la lectura es sospechosa)
    minimo, maximo = UMBRALES[lectura.sensor]
    if lectura.valor < minimo:
        nuevo, detalle = "BAJO", f"< {minimo}{nota}"
    elif lectura.valor > maximo:
        nuevo, detalle = "ALTO", f"> {maximo}{nota}"
    else:
        nuevo, detalle = "NORMAL", ""

    clave = (lectura.pozo_id, lectura.sensor)
    ant_estado = estado_umbral.get(clave, "NORMAL") #si no tiene estado es xq es nuevo, entonces le damos normal en caso de ser null

    if nuevo == ant_estado:
        return #no informamos alerta si no hubo cambio de estado, evitamos saturar

    estado_umbral[clave] = nuevo
    if nuevo == "NORMAL":
        avisar("NORMALIZADO", lectura, f"(previamente estaba en {ant_estado})")
    else:
        avisar(f"ALERTA {nuevo}", lectura, detalle)


def revisar_faltantes(ultimo_visto, sin_datos):
    #se mide con la hora de LLEGADA (monotonic), no con el ts del mensaje: la pregunta es "me estan llegando datos?"
    #solo vigila los sensores que vio desde que arranco
    ahora = time.monotonic()
    for clave, visto in ultimo_visto.items():
        if clave not in sin_datos and ahora - visto > FALTANTE_S:
            sin_datos.add(clave) #se avisa una sola vez, hasta que vuelva el dato
            pozo, sensor = clave
            emitir(f"{hora()} {'SIN DATOS':<12} {pozo} {sensor} (hace {ahora - visto:.0f} s que no llega)")


def confirmar(consumer, asincronico=True):
    try:
        consumer.commit(asynchronous=asincronico)
    except KafkaException as e:
        #no hay nada nuevo que confirmar: no es un error
        if e.args[0].code() != KafkaError._NO_OFFSET:
            raise


def main():
    estado_umbral = {}   # (pozo, sensor) -> "NORMAL" | "ALTO" | "BAJO"
    ultimo_visto  = {}   # (pozo, sensor) -> time.monotonic() de la última llegada
    sin_datos     = set()  # (pozo, sensor) que ya avisaron "sin datos"
    #(pozo, sensor) -> calidad.Previo. Mismo calidad.py que v1, pero con estado propio en memoria:
    #alertas no lee la base ni espera el lote de v1 (al reiniciar se pierde, como el resto del estado)
    estado_calidad = {}

    print("Telegram:", "habilitado" if telegram.habilitado else "deshabilitado (solo log)")
    print(f"Umbrales: {UMBRALES} | sin datos a los {FALTANTE_S:g} s")

    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    ultimo_commit = time.monotonic()
    pendiente = False #hay mensajes leidos sin confirmar

    try:
        while True:
            #timeout de 1 s: aunque no lleguen mensajes, el loop sigue y puede detectar sensores sin datos
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Error de Kafka: {msg.error()}")
                else:
                    pendiente = True
                    try:
                        lectura = validar(msg.key(), msg.value())
                    except MensajeInvalido:
                        lectura = None #invalido: se ignora (v1 ya lo registra / descarta)

                    if lectura is not None:
                        clave = (lectura.pozo_id, lectura.sensor)
                        #el dato llego aunque sea malo: cuenta para "sin datos"
                        ultimo_visto[clave] = time.monotonic()
                        if clave in sin_datos: #volvio el dato
                            sin_datos.discard(clave)
                            avisar("RECUPERADO", lectura)

                        calif, estado_calidad[clave] = calificar(
                            lectura.sensor, lectura.valor, lectura.ts, estado_calidad.get(clave))
                        if calif.calidad == MALA:
                            #no se avisa por una lectura no creible; queda en el log (no va a Telegram)
                            print(f"{hora()} {'IGNORADA':<12} {lectura.pozo_id} {lectura.sensor} "
                                  f"= {lectura.valor:.2f} (mala: {calif.motivo})")
                        else:
                            nota = f" (sospechosa: {calif.motivo})" if calif.calidad == SOSPECHOSA else ""
                            evaluar_umbral(lectura, estado_umbral, nota)

            revisar_faltantes(ultimo_visto, sin_datos)

            #commit periodico y asincronico: no frena el loop
            if pendiente and time.monotonic() - ultimo_commit >= COMMIT_S:
                confirmar(consumer)
                ultimo_commit = time.monotonic()
                pendiente = False

    except KeyboardInterrupt:
        pass
    finally:
        if pendiente:
            try:
                confirmar(consumer, asincronico=False) #al salir, se confirma lo ultimo leido
            except KafkaException as e:
                print(f"No se pudo confirmar el offset al salir: {e}")
        consumer.close()


if __name__ == "__main__":
    main()
