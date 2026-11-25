#este consumidor toma de telemetria raw, y cada 5 min publica su lote en timescaledb

from confluent_kafka import Consumer, KafkaError

import os
from dotenv import load_dotenv
import time
import psycopg
from psycopg import sql
import json
from datetime import datetime
from validacion import validar, MensajeInvalido
from calidad import calificar, N_CONGELADO

load_dotenv()  

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092,localhost:29093,localhost:29094")
GROUP_ID        = os.getenv("GROUP_ID", "v1")
TABLA           = os.getenv("TABLA", "lecturas_v1")
LOTE_S          = int(os.getenv("LOTE_S", "300"))

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB       = os.getenv("POSTGRES_DB", "telemetria")
PG_USER     = os.environ["POSTGRES_USER"]     
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]

LOTE_S=60 #frecuencia con la que se publica el microbatch, defini 1m en llugar de 5m por tema de tiempos en la expo
TOPIC = "telemetria.raw"

INSERT = sql.SQL("""
        INSERT INTO {} (ts, pozo_id, sensor, valor, calidad, calidad_motivo, kafka_part, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pozo_id, ts, sensor) DO NOTHING
    """).format(sql.Identifier(TABLA)) #el on conflit do nothing es la garantia de idempotencia en caso de tener duplicados

#ultimas N_CONGELADO lecturas de cada (pozo, sensor) de la MISMA tabla que escribe este grupo
#(v2 arranca con su tabla vacia, o sea sin estado, igual que si fuera la primera vez)
#el filtro de 8 dias (retencion de telemetria.raw = 7) evita recorrer toda la hypertable
ULTIMAS = sql.SQL("""
        SELECT pozo_id, sensor, ts, valor FROM (
            SELECT pozo_id, sensor, ts, valor,
                   row_number() OVER (PARTITION BY pozo_id, sensor ORDER BY ts DESC) AS n
            FROM {} WHERE ts > now() - INTERVAL '8 days'
        ) t
        WHERE n <= %s
        ORDER BY pozo_id, sensor, ts
    """).format(sql.Identifier(TABLA))

def cargar_estado(conn):
    #el estado de calidad vive en memoria; al arrancar se reconstruye pasando por calificar()
    #las ultimas lecturas guardadas, asi el primer lote se compara contra el ultimo del lote anterior
    estado = {}
    with conn.cursor() as cur:
        cur.execute(ULTIMAS, (N_CONGELADO,))
        for pozo_id, sensor, ts, valor in cur:
            clave = (pozo_id, sensor)
            _, estado[clave] = calificar(sensor, valor, ts, estado.get(clave))
    conn.commit() #cierra la transaccion de lectura
    return estado

def escribir_lote(micro_batch, conn):
    with conn.cursor() as curr:
        curr.executemany(INSERT, micro_batch)
    conn.commit()

def main(): 
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,  
        "group.id": GROUP_ID,                   # "v1"
        "enable.auto.commit": False,            # el offset lo confirmás vos después de escribir
        "auto.offset.reset": "earliest",        # grupo nuevo (v2) arranca desde el offset 0
    }

    conn = psycopg.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                        user=PG_USER, password=PG_PASSWORD)
    #(pozo, sensor) -> calidad.Previo: estado minimo entre lotes para calificar
    estado = cargar_estado(conn)
    print(f"estado de calidad cargado desde {TABLA}: {len(estado)} sensores")

    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    try:
        while True:
            time_pub_microbatch = time.time()
            datos = []
            #el lote califica sobre una COPIA del estado: si el proceso muere antes de escribir,
            #Kafka reentrega el lote y el estado tiene que estar como antes de procesarlo
            #(los Previo son inmutables, alcanza con copiar el dict)
            estado_lote = dict(estado)
            while not (time.time() - time_pub_microbatch >= LOTE_S):
                try:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        print(f"Error: {msg.error()}")
                        continue
                    
                    try:
                        lectura = validar(msg.key(), msg.value())
                    except MensajeInvalido as e:
                        print(f"descartado particion: {msg.partition()} offset: {msg.offset()}: {e}")
                        continue
              
                    clave = (lectura.pozo_id, lectura.sensor)
                    calif, estado_lote[clave] = calificar(
                        lectura.sensor, lectura.valor, lectura.ts, estado_lote.get(clave))

                    datos.append((
                        lectura.ts, lectura.pozo_id, lectura.sensor, lectura.valor,
                        calif.calidad, calif.motivo,
                        msg.partition(), msg.offset()
                    ))
                except (ValueError, KeyError, TypeError) as e:
                    print(f"descartado particion: {msg.partition()} offset: {msg.offset()}: {e}")
            
            #si paso el lapso del LOTE_S entonces publicamos lo que tenemos
            if datos:
                escribir_lote(datos, conn)
                #ahora que tenemos el commit en la bd hacemos el commit del offset confirmado para nusetro concumser group, asi le avisamos a kafka
                consumer.commit(asynchronous=False)
                #recien ahora el lote es definitivo: se adopta el estado de calidad que dejo
                estado = estado_lote
                malas = sum(1 for d in datos if d[4] == "mala")
                print(f"lote: {len(datos)} filas ({malas} malas), commit hecho")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()



if __name__ == "__main__": 
    main()