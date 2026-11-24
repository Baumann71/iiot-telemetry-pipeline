'''
Servidor/esclavo 
simula ser un PLC
es async ya que debe hacer 2 cosas a la vez:
1. atender conexiones y pedidos Modbus del cliente (servee_forever)
2. actualizar los valores del "proceso" de cada pozo cada 1 seg (simular)

Este codigo es un servidor modbus que simula un PLC
'''

import asyncio
import os
import random
from pymodbus.server import ModbusTcpServer
from pymodbus.simulator import DataType, SimData, SimDevice

from pymodbus.client import AsyncModbusTcpClient

HOST = "127.0.0.1"
PORT = 5020

#cantidad de pozos (cada pozo = un SimDevice con su unit id 1..N)
N_POZOS = int(os.getenv("N_POZOS", "6"))

#mapa de registros de cada "PLC" (pozo), igual en todos los devices
#cada float ocupa 2 registros cada uno 16 bits el float es de 32
DIR_PRESION = 0   #registros 0-1, bar
DIR_TEMP    = 2   #registros 2-3, °C
DIR_CAUDAL  = 4   #registros 4-5, m3/dia

#rangos fisicos para acotar los valores (min, max)
LIMITES = {
    "presion": (0.0, 150.0),
    "temp":    (0.0, 120.0),
    "caudal":  (0.0, 400.0),
}

def _a_registros(valor):
    #un float32 -> 2 registros de 16 bits
    return AsyncModbusTcpClient.convert_to_registers(valor, AsyncModbusTcpClient.DATATYPE.FLOAT32)

async def simular(server, periodo=1.0):
    '''
    Actualiza todos los pozos en cada tick (no uno random) para que ninguno quede con valores viejos.
    Cada pozo tiene su propio estado y valores base distintos, asi no dan la misma curva.
    Modelo: caminata aleatoria con reversion a la media
        x += k * (base - x) + ruido
    el termino k*(base - x) tira el valor hacia su base, sin eso con el tiempo el ruido lo haria derivar sin limite
    '''
    #los ids salen del propio servidor (los SimDevice que se le pasaron en main)
    #server.context es un SimCore y device_ids() devuelve los unit ids
    #no es API documentada, por eso pymodbus va con version fija (probado con 3.16.2)
    #se lee una sola vez: los devices no cambian mientras corre el servidor
    ids = server.context.device_ids()

    base = {
        i: {
            "presion": random.uniform(30, 60),
            "temp":    random.uniform(50, 80),
            "caudal":  random.uniform(80, 200),
        }
        for i in ids
    }
    #el estado arranca en la base de cada pozo
    estado = {i: dict(base[i]) for i in ids}

    #cuanto tira hacia la base (k) y cuanto ruido hay por tick, por variable
    k = 0.05
    ruido = {"presion": 0.8, "temp": 0.3, "caudal": 3.0}

    while True:
        for i in ids:
            e = estado[i]
            for var in e:
                e[var] += k * (base[i][var] - e[var]) + random.uniform(-ruido[var], ruido[var])
                lo, hi = LIMITES[var]
                e[var] = max(lo, min(hi, e[var]))

            #escribimos en el datastore del esclavo
            #tabla 4 = input registers, i = unit id del pozo
            await server.async_setValues(i, 4, DIR_PRESION, _a_registros(e["presion"]))
            await server.async_setValues(i, 4, DIR_TEMP,    _a_registros(e["temp"]))
            await server.async_setValues(i, 4, DIR_CAUDAL,  _a_registros(e["caudal"]))

        await asyncio.sleep(periodo) #cede el control al event loop, sin esto se queda siempre ejecutando el loop y nunca alcanza a ejecutar el serve_forever el cual abre el puerto, entonces como no abre el puerto no tiene comunicacion
                                # y se queda actualizando los valores de la tabla nomas
                                # aparte el await server.async_setValues se ejecuta tan rapido que no da tiempo para llegar a ejecutar el serve_forever

async def main(cant_units):
    ''' analogias
    SimDevice = PLC
    SimData = su mapa de memoria,
    ModbusTCPServer= el modulo de red que atiende conexiones
    
    ModbusTcpServer no es el PLC: es el servidor de red, o sea el puerto que escucha. Puede alojar varios dispositivos, cada uno con su SimDevice y su unit id, y cada SimDevice con sus propios SimData
        ModbusTcpServer (una IP:puerto)
        ├─ SimDevice id=1 → SimData(...), SimData(...)
        └─ SimDevice id=2 → SimData(...)
    '''
    devices = []
    for id_dev in range(1, cant_units +1 ):
        #sim device es el equipo completo, x ej un PLC
        dev = SimDevice(
            id=id_dev,
            #sim data es una lista de bloques, el equipo podria tener un sim data para sensores y otro para comandos, con distintos tipos
            simdata=[SimData(0,count=10, values=0, datatype=DataType.REGISTERS)],
            #0: dir inicial; count=10 cant de registros 0..9; values= 0 valor de inicial de cada reg
            #datatype=REGISTERS: son registros de 16 bits crudos
        )
        devices.append(dev)
        
    #instanciamos el server con todos los devices conectados
    server =  ModbusTcpServer(context=devices, address=(HOST, PORT))
    
    #esto deja la tarea en la cola del event loop, esta tarea recien se ejecuta cuando se le cede el control de nuevo al event loop y la prox tarea a ejecutar es esta
    #ocurre cuando el serve_forever tenga que esperar x ej para aceptar conexiones , ahi se cede el control al event looop y se empieza a ejecutar la tarea pendiente simular(server)
    #luego simular server tras actualizar la tabla de input register hace un asyncio.sleep para que ceda el control al event loop x ej para aceptar una comunicacion o algo realzionado con el serve_forever
    asyncio.create_task(simular(server))
    
    print(f"Servidor modbus escuchando en: {HOST}:{PORT} (unit ids 1..{cant_units})")
    
    #el serve_forever abre el socket TCP y empieza a aceptar conexiones y responder pedidos de modbus
    # este no termina de ejecutarse nunca porque queda esperando hasta que alguien lo detenga
    await server.serve_forever()

        
if __name__ == "__main__":
    try:
        asyncio.run(main(N_POZOS))
    except KeyboardInterrupt:
        pass


    
    
    