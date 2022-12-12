from flask import Flask, request, jsonify
from flask_cors import CORS
import time
import requests
import paho.mqtt.client as mqtt
import json
import uuid
import pandas as pd
from pandas import json_normalize
from queue import Queue


class Agregator():
    # TODO: Generator zapisuje dla każdego generatora dane osobno
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()

        self.register_channel = "mqtt"
        self.register_topic = "clock-76467"

        self.server = Flask(str(self.uuid))
        CORS(self.server)

        self.active = True

        self.memory_queue = pd.DataFrame()

        self.last_data = [None]

        self._config = {
            'method': 'http',
            'frequency': 0,
            'http':{
                'destiantion': '127.0.0.1',
                'destiantion_port': 5000,
                'destiantion_path': '/'
            },
            'mqtt': {
                'broker': 'test.mosquitto.org',
                'broker_port': 8080,
                'send_topic': 'baltazar',
                'recive_topic': 'black_1965',
            },
            'constraints': {
                'select': '',
                'function': '',
            }
        }

        # MQTT connection callbacks
        @staticmethod
        def on_connect(mqtt_client, userdata, flags, rc):
            mqtt_client.subscribe(self._config['mqtt']['recive_topic'])
            print('Connection esablished with code: ' + str(rc))
            # mqtt_client.subscribe("black_4567")

        @staticmethod
        def on_publish(client, userdata, mid):
            print(">> SENT MQTT: {}".format(mid))

        @staticmethod
        def on_message(client, userdata, msg):
            # temp = msg.payload.decode()
            if msg.payload.decode() == "##START##":
                self.emit()
            elif msg.payload.decode() == "##STOP##":
                self.stop_emit()
            else:
                print('Message arrived')
                data_json = json.loads(msg.payload.decode())
                self.agregate(data_json)
                # print(self.memory_queue)



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

        # @self.server.route('/register')
        # def register():
        #     # TODO: add register
        #     pass


        self.mqtt_client = mqtt.Client(client_id=str(self.uuid), transport='websockets')
        self.mqtt_client.loop_start()
        self.mqtt_client.on_publish=on_publish
        self.mqtt_client.on_connect=on_connect
        self.mqtt_client.on_message=on_message
        self.mqtt_client.connect(self._config['mqtt']['broker'], int(self._config['mqtt']['broker_port']))
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
            self.memory_queue= pd.concat([self.memory_queue, df], ignore_index=True)
        print(self.memory_queue)
        return None

    def selection(self, selection: str, group_function: str) -> pd.DataFrame:
        if selection != '':
            temp_memory = self.memory_queue.copy()
            temp_memory = temp_memory[eval(selection)]
            if group_function != '':
                temp_memory = eval(temp_memory + "." + group_function + '()')
        return temp_memory

    def package(self):
        pack = Queue()
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
            pack.put(t_str)
        return pack

    def register(self):
        pass

    def http(self):
        data = self.package()
        while self.active:
            if data.qsize() != 0:
                pload = {'data': data.get()}
                content = 'http://' + self._config['http']['destiantion'] +":"+ str(self._config['http']['destiantion_port']) + str(self._config['http']['destiantion_path'])
                requests.post(content, data = pload)
                print(">> SENT HTTP: {} | {}".format(content, json.dumps(pload)))
                time.sleep(self._config['frequency'])
                self.active = True
            else:
                self.active = False
                break

    def mqtt(self):
        data = self.package()
        while self.active:
            if data.qsize() != 0:
                content = (self._config['mqtt']['send_topic'], json.dumps({'data': data.get()}))
                self.mqtt_client.publish(content[0], content[1])
                time.sleep(self._config['frequency'])
                self.active = True
            else:
                self.active = False
                break

    def emit(self):
        if self._config['method'].lower() == 'http':
            self.http()
        if self._config['method'].lower() == 'mqtt':
            self.mqtt()


    def stop_emit(self):
        self.active = False

        

if __name__ == "__main__":
    p = Agregator()
