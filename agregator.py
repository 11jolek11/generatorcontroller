from flask import Flask, request
from flask_cors import CORS
import time
import requests
import paho.mqtt.client as mqtt
import json
import uuid


class Agregator():
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()
        self.server = Flask(str(self.uuid))
        CORS(self.server)
        self.mqtt_client = mqtt.Client(client_id=str(self.uuid), transport='websockets')

        self._conf = {
            'method': 'HTTP',
            'target': '',
            'port': 1883,

        }

        # Routing
        @self.server.route('/')
        def intercept():
            pass

        @self.server.route('/info')
        def info():
            pass

        @self.server.route('/config')
        def change_config():
            pass

    # MQTT connection hooks
    @staticmethod
    def on_connect():
        pass

    @staticmethod
    def on_publish():
        pass

    @staticmethod
    def on_message():
        pass
