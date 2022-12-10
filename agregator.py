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
        self.mqtt_client = mqtt.Client(client_id=str(self.uuid), transport='websockets')

        self.memory_queue = pd.DataFrame()

        self.last_data = [None]

        self.conf = {
            'method': 'http',
            'http':{
                'destiantion': '127.0.0.1',
                'destiantion_port': 9000
            },
            'mqtt': {
                'broker': 'test.mosquitto.org',
                'broker_port': 1883,
                'topic': 'baltazar'
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
            
        @self.server.route('/info')
        def info():
            pass

        @self.server.route('/config')
        def change_config():
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
            print('#################')
            print(df)
            self.memory_queue= pd.concat([self.memory_queue, df], ignore_index=True)
            print('#################')
            print(self.memory_queue)
        return None

    def selection(self, selection: str, group_function: str) -> pd.DataFrame:
        memory = self.memory_queue.copy()
        memory = memory[eval(selection)]
        memory = eval(group_function + '(memory)')
        return memory


    # MQTT connection triggers
    def on_connect(client, userdata, flags, rc, self):
        client.subscribe(self.config['mqtt']['topic'])
        print('Connection esablished with code: ' + str(rc))

    def on_publish(client, userdata, mid):
        pass

    # TODO: self parameter can create problems
    def on_message(client, userdata, msg, self):
        data_json = json.loads(msg.payload.decode())
        self.agregate(data_json)
        

if __name__ == "__main__":
    Agregator()