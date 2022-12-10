from flask import Flask, request
from flask_cors import CORS
import time
import requests
import paho.mqtt.client as mqtt
import json
import uuid
import pandas as pd
from pandas.io.json import json_normalize


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
            # TODO: do agragacji danych i ich obróbki użyj dataframe z pandas
            data = request.form['data']
            data_json = json.loads(data)
            # FIXME: find better solution:
            if len(self.last_data) > 1:
                if data_json.keys() != self.last_data[-1].keys():
                    # Clear DataFrame and prepare for next type of data
                    self.memory_queue = self.memory_queue[0:0]
            self.last_data.append(data_json)
            df = json_normalize(data_json)
            # print('###$')
            # print(df)
            if self.memory_queue.empty:
                self.memory_queue = df
            else:
                # print(self.memory_queue)
                print('#################')
                print(df)
                # FIXME: Join on 1st column
                # self.memory_queue = self.memory_queue.join(df, on=df.columns.values.tolist()[0])

                self.memory_queue= pd.concat([self.memory_queue, df], ignore_index=True)
                print('#################')
                print(self.memory_queue)
            return """HTML"""
            
            # print(self.memory_queue)

        @self.server.route('/info')
        def info():
            pass

        @self.server.route('/config')
        def change_config():
            pass


        self.server.run(port=9000, debug=True)

    # MQTT connection hooks
    @staticmethod
    def on_connect(client, userdata, flags, rc):
        pass

    @staticmethod
    def on_publish(client, userdata, mid):
        pass

    @staticmethod
    def on_message(client, userdata, msg):
        pass

if __name__ == "__main__":
    Agregator()