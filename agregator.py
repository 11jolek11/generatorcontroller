from flask import Flask, request, jsonify
from flask_cors import CORS
import time
import requests
import paho.mqtt.client as mqtt
import json
import uuid
import pandas as pd
from pandas import json_normalize


class Agregator():
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()

        self.server = Flask(str(self.uuid))
        CORS(self.server)

        self.active = True

        self.mqtt_client = mqtt.Client(client_id=str(self.uuid), transport='websockets')

        self.memory_queue = pd.DataFrame()

        self.last_data = [None]

        self._config = {
            'method': 'http',
            'frequency': 1,
            'http':{
                'destiantion': '127.0.0.1',
                'destiantion_port': 5000,
                'destiantion_path': '/'
            },
            'mqtt': {
                'broker': 'test.mosquitto.org',
                'broker_port': 1883,
                'topic': 'baltazar'
            },
            'constraints': {
                'select': '',
                'function': '',
            }
        }



        # Routing
        @self.server.route('/', methods=['post'])
        def intercept():
            data = request.form['data']
            data_json = json.loads(data)
            # FIXME: find better solution:
            self.agregate(data_json)
            return jsonify({'status': 'Ok'})

        @self.server.route('/start', methods=['get', 'post'])
        def start():
            self.active = True
            self.emit()
            return jsonify({'status': 'START'})

        @self.server.route('/stop', methods=['post', ' get'])
        def stop():
            self.active = False
            self.stop_emit()
            return jsonify({'status': 'STOP'})
            
        @self.server.route('/info')
        def info():
            # TODO: add info section
            pass

        @self.server.route('/config')
        def change_config():
            # TODO: add config
            pass


        self.server.run(port=9000, debug=True)

    def agregate(self, data_json: dict) -> None:
        if len(self.last_data) > 1:
            if data_json.keys() != self.last_data[-1].keys():
                # Clear DataFrame and prepare for next type of data
                self.memory_queue = self.memory_queue[0:0]
        self.last_data.append(data_json)
        df = json_normalize(data_json)
        if self.memory_queue.empty:
            self.memory_queue = df
        else:
            print(df)
            self.memory_queue= pd.concat([self.memory_queue, df], ignore_index=True)
        return None

    def selection(self, selection: str, group_function: str) -> pd.DataFrame:
        if selection != '':
            temp_memory = self.memory_queue.copy()
            temp_memory = temp_memory[eval(selection)]
            if group_function != '':
                temp_memory = eval(temp_memory + "." + group_function + '()')
        return temp_memory

    def package(self):
        pack = []
        if self._config['constraints']['select'] != '' or self._config['constraints']['function'] != '':
            data = self.selection(self._config['constraints']['select'], self._config['constraints']['function'])
        else:
            data = self.memory_queue.copy()
        for y in range(data.shape[0]):
            t_str='{'
            for x in data.columns.tolist():
                t_str += '"' + str(x) + '"'  + ":" + '"' + str(data[x][y]) + '"' + ","
            t_str = t_str[:-1:]
            t_str += '}'
            pack.append(t_str)
        return pack

    # MQTT connection triggers
    def on_connect(client, userdata, flags, rc, self):
        client.subscribe(self._config['mqtt']['topic'])
        print('Connection esablished with code: ' + str(rc))

    def on_publish(client, userdata, mid):
        pass

    # TODO: self parameter can create problems
    def on_message(client, userdata, msg, self):
        data_json = json.loads(msg.payload.decode())
        self.agregate(data_json)

    def http(self):
        data = self.package()
        while self.active:
            print(">> SENT")
            try:
                pload = {'data': data}
            except StopIteration:
                self.active = False
                break
            requests.post('http://' + self._config['http']['destiantion'] +":"+ str(self._config['http']['destiantion_port']) + str(self._config['http']['destiantion_path']), data = pload)
            time.sleep(self._config['frequency'])

    def mqtt(self):
        # FIXME: fixes needed 
        data = self.package()
        self.mqtt_client.on_publish=self.on_publish
        self.mqtt_client.on_connect=self.on_connect
        self.mqtt_client.connect(self._config['mqtt']['broker'], int(self._config['mqtt']['port']))
        while self.active:
            try:
                self.mqtt_client.publish(self._config['mqtt']['topic'], json.dumps({'data': next(data)}))
            except StopIteration:
                self.active = False
                break
            time.sleep(self._config['frequency'])
        self.mqtt_client.disconnect()

    def emit(self):
        if self._config['method'].lower() == 'http':
            self.http()
        if self._config['method'].lower() == 'mqtt':
            self.mqtt()


    def stop_emit(self):
        self.active = False

        

if __name__ == "__main__":
    p = Agregator()
    # p.emit()