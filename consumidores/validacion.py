"""
Validacion de los mensajes de telemetria.raw, compartida por v1/v2 y alertas.

Valida que el mensaje este bien formado y que el valor sea FISICAMENTE POSIBLE.
No decide si el valor es normal o peligroso: eso es trabajo de alertas.
    - presion = 10000 bar   -> invalido (imposible, sensor roto o mensaje corrupto) -> se descarta
    - presion = 152 bar     -> valido (es un dato real, fuera de operacion) -> se guarda y alertas avisa
Si se mezclaran las dos cosas, una presion alta de verdad se descartaria en silencio.

Uso:
    try:
        lectura = validar(msg.key(), msg.value())
    except MensajeInvalido as e:
        print(f"descartado: {e}")
"""

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

SENSORES = ("presion", "temperatura", "caudal")

#rangos fisicamente posibles (min, max), bien amplios a proposito:
#solo atrapan valores absurdos, los umbrales de operacion estan en alertas
RANGO_FISICO = {
    "presion":     (0.0, 1000.0),    # bar
    "temperatura": (-50.0, 300.0),   # °C
    "caudal":      (0.0, 10000.0),   # m3/dia
}

#un ts mas adelante que esto respecto de ahora es un reloj mal configurado
#los ts viejos si se aceptan: el replay de v2 lee historia
MAX_FUTURO = timedelta(minutes=5)

PATRON_POZO = re.compile(r"^pozo-\d+$")



class MensajeInvalido(ValueError):
    """El mensaje no se puede usar. El texto de la excepcion es el motivo."""


@dataclass(frozen=True)
class Lectura:
    pozo_id: str
    sensor: str
    valor: float
    ts: datetime        # siempre con zona horaria


def validar(key, value):
    """
    key, value: bytes crudos del mensaje de Kafka (msg.key(), msg.value()), pueden ser None.
    Devuelve una Lectura o lanza MensajeInvalido con el motivo.
    """
    if value is None:
        raise MensajeInvalido("mensaje vacio")

    try:
        d = json.loads(value)           # json.loads acepta bytes en utf-8
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise MensajeInvalido(f"JSON invalido: {e}") from None

    if not isinstance(d, dict):
        raise MensajeInvalido("el payload no es un objeto JSON")

    faltan = [c for c in ("pozo_id", "sensor", "valor", "ts") if c not in d]
    if faltan:
        raise MensajeInvalido(f"faltan campos: {', '.join(faltan)}")

    #pozo_id
    pozo_id = d["pozo_id"]
    if not isinstance(pozo_id, str) or not PATRON_POZO.match(pozo_id):
        raise MensajeInvalido(f"pozo_id invalido: {pozo_id!r}")

    #la key la pone EMQX a partir del topic MQTT: si no coincide con el payload,
    #el mensaje se publico en el topic de otro pozo y quedo en la particion equivocada
    clave = key.decode("utf-8", errors="replace") if key is not None else None
    if clave != pozo_id:
        raise MensajeInvalido(f"key {clave!r} distinta de pozo_id {pozo_id!r}")

    #sensor
    sensor = d["sensor"]
    if sensor not in SENSORES:
        raise MensajeInvalido(f"sensor desconocido: {sensor!r}")

    #valor: numero de verdad (no string, no bool: en python True es un int), finito y en rango
    valor = d["valor"]
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        raise MensajeInvalido(f"valor no numerico: {valor!r}")
    valor = float(valor)
    if not math.isfinite(valor):
        raise MensajeInvalido(f"valor no finito: {valor}")
    lo, hi = RANGO_FISICO[sensor]
    if not lo <= valor <= hi:
        raise MensajeInvalido(f"{sensor} fuera de rango fisico [{lo}, {hi}]: {valor}")

    #ts: ISO 8601 con zona horaria (sin zona, postgres lo tomaria como hora local)
    if not isinstance(d["ts"], str):
        raise MensajeInvalido(f"ts no es texto: {d['ts']!r}")
    try:
        ts = datetime.fromisoformat(d["ts"])
    except ValueError:
        raise MensajeInvalido(f"ts no es una fecha ISO: {d['ts']!r}") from None
    if ts.tzinfo is None:
        raise MensajeInvalido(f"ts sin zona horaria: {d['ts']!r}")
    if ts > datetime.now(timezone.utc) + MAX_FUTURO:
        raise MensajeInvalido(f"ts en el futuro: {d['ts']!r}")

    
    return Lectura(pozo_id=pozo_id, sensor=sensor, valor=valor, ts=ts)
