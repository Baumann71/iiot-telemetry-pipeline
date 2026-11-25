"""
Envio de alertas a Telegram (extra), usado por alertas.py desde avisar().

- Opcional: sin TELEGRAM_TOKEN y TELEGRAM_CHAT_ID en el entorno no envia nada y
  alertas funciona igual (quien corrige el repo no tiene el bot).
- No bloquea al consumidor: enviar() solo deja el texto en una cola; un hilo aparte
  hace la llamada HTTPS. Si Telegram tarda o esta caido, el poll() de Kafka sigue.
- Agrupa rafagas: junta lo que llega en ESPERA_S segundos en un solo mensaje
  (por ej. los 18 "SIN DATOS" cuando se corta el gateway).
- Si falla el envio se registra y se sigue: la alerta igual quedo en el log de alertas.
- El hilo es daemon: al cerrar el proceso, lo que quedaba en la cola se pierde.

Usa solo la biblioteca estandar (urllib), sin dependencias nuevas.
"""

import json
import os
import queue
import threading
import time
import urllib.error
import urllib.request

API = "https://api.telegram.org"
LIMITE_TEXTO = 4000     # Telegram acepta hasta 4096 caracteres por mensaje


class Telegram:
    def __init__(self, token, chat_id, espera_s=2.0, timeout_s=5.0):
        self._token = token
        self._chat_id = chat_id
        self._espera_s = espera_s
        self._timeout_s = timeout_s
        self._cola = queue.Queue(maxsize=1000)

        if self.habilitado:
            threading.Thread(target=self._trabajador, name="telegram", daemon=True).start()

    @property
    def habilitado(self):
        return bool(self._token and self._chat_id)

    def enviar(self, texto):
        """No bloquea. Si la cola esta llena (Telegram caido mucho tiempo), descarta el mensaje."""
        if not self.habilitado:
            return
        try:
            self._cola.put_nowait(texto)
        except queue.Full:
            print("[telegram] cola llena, mensaje descartado (la alerta igual quedo en el log)")

    def _trabajador(self):
        while True:
            lineas = [self._cola.get()]             # espera hasta que haya algo
            #junta lo que siga llegando durante espera_s, para mandar un solo mensaje
            limite = time.monotonic() + self._espera_s
            while (resto := limite - time.monotonic()) > 0:
                try:
                    lineas.append(self._cola.get(timeout=resto))
                except queue.Empty:
                    break
            for trozo in _partir(lineas, LIMITE_TEXTO):
                self._post(trozo)

    def _post(self, texto):
        #ojo: la url lleva el token, por eso nunca se imprime la url ni la excepcion completa
        url = f"{API}/bot{self._token}/sendMessage"
        cuerpo = json.dumps({"chat_id": self._chat_id, "text": texto}).encode("utf-8")

        #errores de red (corte, DNS, timeout) se reintentan con espera creciente;
        #corre en el hilo de telegram, asi que no frena al consumidor
        esperas = (2, 4)    #3 intentos en total
        for intento in range(len(esperas) + 1):
            req = urllib.request.Request(url, data=cuerpo, headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=self._timeout_s) as r:
                    r.read()
                if intento:
                    print(f"[telegram] enviado en el intento {intento + 1}")
                return
            except urllib.error.HTTPError as e:
                if e.code == 429 and intento < len(esperas):
                    #demasiados mensajes: Telegram dice cuanto esperar
                    espera = _retry_after(e)
                    print(f"[telegram] limite de envios, reintento en {espera} s")
                    time.sleep(espera)
                    continue
                #otro HTTP (401 token invalido, 400 chat_id invalido): reintentar no lo arregla
                print(f"[telegram] envio rechazado: HTTP {e.code}")
                return
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                #e.reason es el motivo de red (ej. "getaddrinfo failed", "timed out"), no incluye la url
                motivo = getattr(e, "reason", e)
                if intento < len(esperas):
                    print(f"[telegram] fallo de red ({motivo}), reintento en {esperas[intento]} s")
                    time.sleep(esperas[intento])
                else:
                    print(f"[telegram] no se pudo enviar tras {intento + 1} intentos: {motivo}"
                          " (la alerta igual quedo en el log)")


def _partir(lineas, limite):
    """Une las lineas en textos de hasta 'limite' caracteres sin cortar una linea al medio."""
    trozo = ""
    for linea in lineas:
        linea = linea[:limite]
        if trozo and len(trozo) + 1 + len(linea) > limite:
            yield trozo
            trozo = ""
        trozo = f"{trozo}\n{linea}" if trozo else linea
    if trozo:
        yield trozo


def _retry_after(error):
    try:
        return int(json.loads(error.read())["parameters"]["retry_after"])
    except Exception:
        return 5


def desde_entorno():
    """Crea el notificador con TELEGRAM_TOKEN y TELEGRAM_CHAT_ID (si faltan, queda deshabilitado)."""
    return Telegram(os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHAT_ID"))
