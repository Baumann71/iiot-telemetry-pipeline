#es el maestro
import asyncio
import pymodbus.client as ModbusClient

from pymodbus import FramerType

async def leer_planta(client, device_id):
                  #read_input_registers(address: int, *, count: int = 1, device_id: int = 1, no_response_expected: bool = False)→ T
    refg = await client.read_input_registers(0, count=6, device_id=device_id)#0 dice la dir del 1er registro desde el cual empezas a leer
        #count 6 cuantos reg segui2 leera partir de esa direccion, o sea 0..5
        #device_id el numero del equipo al que le preguntamos (el esclavo), o sea el pozo
        #mapa de registros (igual que en server.py), cada float32 ocupa 2 registros de 16 bits:
            # registros 0-1: presion (bar)
            # registros 2-3: temperatura (°C)
            # registros 4-5: caudal (m3/dia)

    if refg.isError():
        raise RuntimeError(f"error Modbus: {refg}")

    #client.convert_from_registers se usa para transformar registros modbus que al ser del input registers
    #cada registro es de 16 bits, en tipos de datos comunes en python como float/enteros de 42 o 64 bits o cadenas de texto
    # por lo que un float de 32 bits debe combinar 2 registros
    presion = client.convert_from_registers(refg.registers[0:2], client.DATATYPE.FLOAT32)
    temp = client.convert_from_registers(refg.registers[2:4], client.DATATYPE.FLOAT32)
    caudal = client.convert_from_registers(refg.registers[4:6], client.DATATYPE.FLOAT32)
    return presion, temp, caudal

def crear_cliente(host, port)-> ModbusClient.ModbusBaseClient:
    return ModbusClient.AsyncModbusTcpClient(
        host,
        port= port,
        
        #esto le dice al modbus tcp que empaquete con un frame tcp
        #por default ya se hace asi pero puede haber casos donde el frame sea x ej  Modbus RTU over TCP
        framer= FramerType.SOCKET,
        timeout=5, #tiemp max seg que el cliente espera la rta del servidor (o por establecer la conexion inicial)
        retries=1 # cant veces que se intenta reenviar automaicamente la peticion si falla debido a un problema de red o timeout
        
        #source_address=()  esto es la ip,puerto de la interfaz de red que inicia la conexion tcp (routing table,  puerto efimero etc)
            # solo se usa si hay varias interfaces de red, al no ponerlo se le encarga al SO que lo gestione solo
        
        #reconnect_delay =0.1, #espera inicial entre reintentos, con 0 se desactiva la reconexion automatica
        #reconnect_delay_max = 300, #tope de ms que espera 
        #tambien tiene un backoff, por cada intento fallido duplica el tiempo de espera
    )
    
async def run_async_client_tcp(host, port):        
    print("iniciando conexion cliente")
    #el host, port es del destino o sea el servidor/esclavo
    client = crear_cliente(host, port)
    
    await client.connect()
    print("cliente conectado")
    
    assert client.connect #esto es una condicion, tal que si el cliente esta conectado da true, caso contratio tira una excep
    
    try:
        #ejecutamos polling
        while True:
            try:
                presion, temp, caudal = await leer_planta(client, UNIT_ID)
                print(f"presion={presion:6.2f} bar  temperatura={temp:5.1f} C  caudal={caudal:6.1f} m3/d")
            except Exception as e:
                print("fallo en lectura", e)
        
            await asyncio.sleep(TIME_SLEEP) #cede el control al event loop, el loop ejecuta lo que esta pendiente
    finally:
        client.close()
                
if "__main__" == __name__:
    try:
        print("comenzo!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n\n")
        
        TIME_SLEEP = 1
        UNIT_ID = 1
        HOST= "127.0.0.1"
        PORT= 5020

        asyncio.run(run_async_client_tcp(HOST, PORT))
    except KeyboardInterrupt:
        pass