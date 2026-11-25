"""
Calidad de las lecturas VALIDAS (las invalidas ya las saco validacion.py), compartida por v1/v2 y alertas.

    buena      -> todo normal
    sospechosa -> creible, pero conviene mirarla (salto grande pero posible, instrumento saturado, duplicado)
    mala       -> fisicamente posible pero no creible (valor congelado, salto imposible)

Una lectura mala se guarda igual (no se pierde en silencio), pero alertas no avisa por ella.

Las reglas comparan contra la lectura anterior del mismo (pozo, sensor): ese es el "estado minimo
entre lotes" (Previo). Es una funcion pura, sin Kafka ni base: el que la llama guarda el estado.

Uso:
    calif, nuevo = calificar(lectura.sensor, lectura.valor, lectura.ts, estado.get(clave))
    estado[clave] = nuevo
"""

from dataclasses import dataclass
from datetime import datetime

BUENA, SOSPECHOSA, MALA = "buena", "sospechosa", "mala"

#rango del instrumento (el mismo que LIMITES de gateway/server.py): un valor pegado al borde
#es un sensor saturado, el valor real puede estar mas alla
RANGO_INSTRUMENTO = {
    "presion":     (0.0, 150.0),    # bar
    "temperatura": (0.0, 120.0),    # °C
    "caudal":      (0.0, 400.0),    # m3/dia
}
MARGEN_SATURADO = 0.005   # a menos del 0,5 % del rango del borde = saturado

#velocidad de cambio maxima (unidades por segundo): (sospechosa, mala)
#calibrado contra el simulador: el peor salto de un evento real (EVENTOS de server.py, k=0.3,
#5 s entre lecturas) da ~17 bar/s, ~10 °C/s y ~32 m3/dia/s. Los eventos quedan como sospechosa
#(alertas avisa igual) y "mala" esta a mas del doble: la calidad nunca tiene que tapar una alarma real
SALTO_MAX = {
    "presion":     (10.0, 40.0),
    "temperatura": (5.0, 25.0),
    "caudal":      (20.0, 80.0),
}
#lecturas casi simultaneas darian velocidades enormes por dividir por ~0: se toma al menos 1 s
DT_MIN_S = 1.0

#el mismo valor exacto N veces seguidas = sensor congelado (un sensor real siempre tiene ruido)
#con el gateway cada 5 s son 30 s sin cambiar ni un decimal
N_CONGELADO = 6


@dataclass(frozen=True)
class Previo:
    """Estado por (pozo, sensor). Inmutable: se puede copiar el dict de estados sin copiar cada uno."""
    ts: datetime            # ultima lectura vista
    valor: float
    repeticiones: int       # cuantas veces seguidas se repitio `valor` (incluida la ultima)
    ref_ts: datetime        # ultima lectura NO mala: contra esta se mide el salto
    ref_valor: float


@dataclass(frozen=True)
class Calificacion:
    calidad: str
    motivo: str | None = None


def calificar(sensor, valor, ts, previo):
    """
    sensor, valor, ts: de una Lectura ya validada.
    previo: Previo de ese (pozo, sensor), o None si es la primera lectura.
    Devuelve (Calificacion, Previo nuevo).
    """
    #duplicado (el productor de EMQX duplica en un failover) o fuera de orden: no se toca el
    #estado, si no un duplicado contaria como "valor repetido" y empujaria hacia "congelado"
    if previo is not None and ts <= previo.ts:
        return Calificacion(SOSPECHOSA, "duplicada o fuera de orden"), previo

    repeticiones = previo.repeticiones + 1 if previo is not None and valor == previo.valor else 1

    calif = _reglas(sensor, valor, ts, previo, repeticiones)

    #el salto se mide contra la ultima lectura no mala: si hubo un pico malo (40 -> 149 -> 41),
    #la vuelta a 41 se compara con 40 y no queda mala tambien. Y si el nivel cambio de verdad,
    #el tiempo desde la referencia crece y la velocidad baja sola
    if calif.calidad == MALA and previo is not None:
        ref_ts, ref_valor = previo.ref_ts, previo.ref_valor
    else:
        ref_ts, ref_valor = ts, valor

    return calif, Previo(ts, valor, repeticiones, ref_ts, ref_valor)


def _reglas(sensor, valor, ts, previo, repeticiones):
    #de la mas grave a la menos grave: gana la primera que aplica
    if repeticiones >= N_CONGELADO:
        return Calificacion(MALA, f"congelado: {repeticiones} lecturas iguales")

    velocidad = None
    if previo is not None:
        dt = max((ts - previo.ref_ts).total_seconds(), DT_MIN_S)
        velocidad = abs(valor - previo.ref_valor) / dt
        mala = SALTO_MAX[sensor][1]
        if velocidad > mala:
            return Calificacion(MALA, f"salto imposible: {velocidad:.1f}/s (max {mala:g}/s)")

    lo, hi = RANGO_INSTRUMENTO[sensor]
    margen = (hi - lo) * MARGEN_SATURADO
    if valor <= lo + margen or valor >= hi - margen:
        return Calificacion(SOSPECHOSA, f"instrumento saturado (rango {lo:g}-{hi:g})")

    if velocidad is not None and velocidad > SALTO_MAX[sensor][0]:
        return Calificacion(SOSPECHOSA, f"salto grande: {velocidad:.1f}/s")

    return Calificacion(BUENA)
