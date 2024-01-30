from contextlib import suppress
from threading import RLock
from typing import ClassVar, Dict, NamedTuple, Tuple
from .client import Client
from paho.mqtt.client import MQTTMessageInfo
from paho.mqtt.properties import Properties
import logging

logger = logging.getLogger(__name__)

class PMsg(NamedTuple):
    topic:str
    payload:str
    qos:int
    retain:bool
    properties:Properties | None


class RepublishClient(Client):
    pmq:Dict[int,Tuple[MQTTMessageInfo, PMsg]] = {}
    _pmq_lock = RLock()
    _logger:ClassVar[logging.Logger] = logger.getChild('RepublishClient')
    MsgCount:int = 0

    def on_connect(self, client, userdata, flags, rc):
        super().on_connect(client, userdata, flags, rc)
        if rc == 0 and self.pmq: self._republish()

    def _republish(self):
        with self._pmq_lock:
            qmsgs, self.pmq = self.pmq, {}
            if not qmsgs: return
            self._logger.debug(f'republishing msgs:{len(qmsgs)}')
            for mi,msg in qmsgs.values():
                with suppress(ValueError, RuntimeError):
                    if mi.is_published(): continue
                self.publish(*msg)
        self._logger.debug('republished messages')

    def on_publish(self, client, userdata, mid):
        with self._pmq_lock:
            self.pmq.pop(mid, None)

    def publish(self, topic:str, payload:str = None, qos:int = 0, retain:bool = False, properties:Properties | None = None) -> MQTTMessageInfo:
        with self._pmq_lock:
            self.MsgCount += 1
            mi = super().publish(*(msg := PMsg(topic, payload, qos, retain, properties)))
            self.pmq[mi.mid] = (mi, msg)
        return mi
