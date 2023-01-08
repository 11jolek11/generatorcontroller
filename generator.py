from datasource.data import DataCSV
import argparse
import time
import requests
import paho.mqtt.client as mqtt
import json
import os


class Generator:
    def __init__(self, config:str) -> None:
        self._config = config
        self._config = json.loads(self._config)

        self.active = True

        self.buffer = []

    # mqtt hook#1
    @staticmethod
    def on_connect(client, userdata, flags, rc):
        print("Connected with result code "+str(rc))

    # mqtt hook#2
    @staticmethod
    def on_publish(client, userdata, mid):
        print("sent to MQTT broker >> " + str(mid))

    def load(self):
        self.temp = iter(self.buffer)
        self._data_config = self._config['data']
        data = DataCSV(self._data_config['source'])
        labels, holder = data.expose()
        dict_len = max(list(holder[labels[0]].keys()))
        for y in range(6000):
            t_str="{"
            y = y % dict_len
            for x in labels:
                t_str += '"' + str(x) + '"'  + ":" + '"' + str(holder[x][y]) + '"' + ","
            t_str = t_str[:-1:]
            t_str += '}'
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
        client.on_publish=self.on_publish
        client.on_connect=self.on_connect
        client.connect(self._mqtt_config['broker'], int(self._mqtt_config['port']))
        while self.active:
            p = next(self.temp)
            client.publish(self._mqtt_config['topic'], json.dumps(p))
            time.sleep(self.frequency)
        client.disconnect()

    def http(self):
        while self.active:
            pload = json.dumps({'data': next(self.temp)})
            url = 'http://' + self._http_config['host'] +":"+ str(self._http_config['port']) + '/'
            headers = {
              'Content-Type': 'application/json'
            }
            try:
                requests.request('POST', url, data=pload, headers=headers)
            except:
                print('\033[91m>> Connection dropped by peer <<\033[0m')
                break
            time.sleep(self.frequency)

    def run(self):
        print("\033[96m>> Generator active <<\033[0m")
        self.load()
        self.send()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', type=str)

    args = parser.parse_args()
    p = Generator(args.config)
    p.run()
