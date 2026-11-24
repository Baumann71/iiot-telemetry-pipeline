#este consumidor toma de telemetria raw, y cada 5 min publica su lote en timescaledb

from confluent_kafka import Consumer, KafkaError

import os
from dotenv import load_dotenv
import time
import psycopg
from psycopg import sql
import json
from datetime import datetime

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
        INSERT INTO {} (ts, pozo_id, sensor, valor, kafka_part, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (pozo_id, ts, sensor) DO NOTHING 
    """).format(sql.Identifier(TABLA)) #el on conflit do nothing es la garantia de idempotencia en caso de tener duplicados

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
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    try:
        while True:
            time_pub_microbatch = time.time()
            datos = []
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

                    clave = msg.key().decode("utf-8") if msg.key() else None
                    valor = msg.value().decode("utf-8") if msg.value() else None
                    print(
                        f"Tópico: {msg.topic()} | Partición: {msg.partition()} | "
                        f"Offset: {msg.offset()} | Clave: {clave} | Valor: {valor}"
                    )
                    
                    
                    d_json = json.loads(valor)
                    fila = (
                        datetime.fromisoformat(d_json["ts"]),   # si no es fecha válida, salta acá
                        d_json["pozo_id"],
                        d_json["sensor"],
                        float(d_json["valor"]),
                        msg.partition(),
                        msg.offset(),
                    )
                    datos.append(fila)
                except (ValueError, KeyError, TypeError) as e:
                    print(f"descartado particion: {msg.partition()} offset: {msg.offset()}: {e}")
            
            #si paso el lapso del LOTE_S entonces publicamos lo que tenemos
            if datos:
                escribir_lote(datos, conn)
                #ahora que tenemos el commit en la bd hacemos el commit del offset confirmado para nusetro concumser group, asi le avisamos a kafka
                consumer.commit(asynchronous=False)
                print(f"lote: {len(datos)} filas, commit hecho")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()



if __name__ == "__main__": 
    main()