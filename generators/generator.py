from datasource.data import DataCSV
import os
import time
import configparser
import requests
import paho.mqtt.client as mqtt
from itertools import count
import argparse
import json
from itertools import count
from sys import maxsize
from flask import Flask, request


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_publish(client, userdata, mid):
    print("SENT >>")


class Generator:
    def __init__(self, source) -> None:
        self.source = source
        self._config = {
            'data': {'source': self.source, 'channel': '', 'frequency': ''},
            'MQTT': {'broker': '', 'port': '', 'topic': ''},
            'HTTP': {'host': '', 'port': ''},
            }

        self.app = Flask(__name__)

        # self._config = config
        self.active = True

        self.buffer = []

    def load(self):
        self.temp = iter(self.buffer)
        self._data_config = self._config['data']
        data = DataCSV(self._data_config['source'])
        labels, holder = data.expose()
        dict_len = max(list(holder[labels[0]].keys()))
        for y in range(6000):
            t_str=""
            y = y % dict_len
            for x in labels:
                t_str += str(holder[x][y]) + "  " 
            self.buffer.append(t_str)

    def send(self):
        self.frequency = 1/float(self._data_config['frequency'])
        self._mqtt_config = self._config['MQTT']
        self._http_config = self._config['HTTP']
        if self._data_config['channel'].lower() == 'mqtt':
            self.mqtt()
        elif self._data_config['channel'].lower() == 'http':
                self.http()

    def mqtt(self):
        client = mqtt.Client()
        client.on_publish=on_publish
        client.on_connect=on_connect
        client.connect(self._mqtt_config['broker'], int(self._mqtt_config['port']))
        # for _ in iter(int, 1):
        while self.active:
            client.publish(self._mqtt_config['topic'], json.dumps({'data': next(self.temp)}))
            time.sleep(self.frequency)
        client.disconnect()

    def http(self):
        # for _ in iter(int, 1):
        while self.active:
            pload = {'data': next(self.temp)}
            requests.post(self._http_config['host'] +":"+ self._http_config['port'], data = pload)
            time.sleep(self.frequency)

    # Routings
    @self.app.route('<source>/start', methods= ['post'])
    # TODO: Posibble error 
    def start(self, source):
            self._config = {
            'data': {'source': source, 'channel': request.form.get('channel'), 'frequency': request.form.get('frequency')},
            'MQTT': {'broker': request.form.get('broker'), 'port': request.form.get('port'), 'topic': request.form.get('topic')},
            'HTTP': {'host': request.form.get('host'), 'port': request.form.get('port')},
            }
            self.load()
            self.send()

    @self.app.route('<source>/stop', methods= ['post'])
    # TODO: Posibble error 
    def stop(self, source):
        self.active = False


class Emitter:
    def __init__(self) -> None:
        pass
        # self._config = {
        #     'data': {'source':'', 'channel': '', 'frequency': ''},
        #     'MQTT': {'broker': '', 'port': '', 'topic': ''},
        #     'HTTP': {'host': '', 'port': ''},
        #     }     



if __name__ == "__main__":
    pass
