import os
import socket
import sys
import paho.mqtt.client as mqtt
# from paho.mqtt.properties import Properties
# try:
# except ImportError:
#     sys.path.append( '/Media/mediaprocessor/packages' )
#     import paho.mqtt.client as mqtt

class Client(mqtt.Client):
    MAX_RECONNECT = 10
    CLIENT_ID = f'{socket.gethostname()}-{os.path.basename(sys.argv[0])}'
    _reconnect_attempts = 0

    def __init__(self, host:str, *,
                 username:str=None, password:str=None,
                 port:int = 1883, keep_alive:int = 60,
                 client_id=None, clean_session=None, userdata=None,
                 protocol=mqtt.MQTTv311, transport="tcp", reconnect_on_failure=True):

        super().__init__(client_id=(client_id or self.CLIENT_ID), clean_session=clean_session, userdata=userdata,
                         protocol=protocol, transport=transport, reconnect_on_failure=reconnect_on_failure)

        self.__attach_handlers()
        self.reconnect_delay_set(1, 30)
        if username or password:
            self.username_pw_set(username, password)
        self.connect_async(host, port, keep_alive)

    def __attach_handlers(self):
        for aname in dir(self):
            if not aname.startswith('on_'): continue
            if not callable(handler := getattr(self,aname)):continue
            setattr(self, '_'+aname, handler)

    @classmethod
    def FromEnv(cls, prefix:str, host:str, username:str, password:str, port:int|str=1883):
        return cls(**{
            'host':os.environ.get(f'{prefix}_HOST', host),
            'port':int(os.environ.get(f'{prefix}_PORT', port)),
            'username':os.environ.get(f'{prefix}_USER',username),
            'password':os.environ.get(f'{prefix}_PASS',password),
        })

    @property
    def name(self):return self.__class__.__name__

    def __log(self, msg, rc=None):
        print(f"{self.name}: {msg}{('' if rc is None else f': {mqtt.error_string(rc)}')}",flush=True)

    def on_connect(self, client, userdata, flags, rc):
        self.__log("Connected" if rc == 0 else "Failed to connect", None if rc == 0 else rc)
        if rc != 0: self._handle_connect_failed()

    def _handle_connect_failed(self):
        self._reconnect_attempts += 1
        if self._reconnect_attempts >= self.MAX_RECONNECT:
            self.__log('Max reconnects exceeded')
            sys.exit(1)

    def on_disconnect(self, client, userdata, rc):
        self.__log("disconnected", rc)

    def on_fail(self, client, userdata):
        self.__log("Failed to connect")
        self._handle_connect_failed()
