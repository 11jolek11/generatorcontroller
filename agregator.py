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
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()

        self.ip = '127.0.0.1'
        self.port = 9000

        self.register_channel = "mqtt"
        self.register_topic = "clock-76467"

        self.server = Flask(str(self.uuid))
        CORS(self.server)

        self.active = True

        # TODO: This flag should be on False in production env
        self.registered = False
        # on true now because debugging purposes
        # self.registered = True

        self.memory_queue = pd.DataFrame()

        self.register_agent = mqtt.Client(client_id=str(self.uuid), transport='websockets')

        self.last_data = [None]

        self.sending = False

        self._config = {
            'method': 'http',
            'frequency': 0,
            'pack_size': 1,
            'http':{
                'destiantion': '127.0.0.1',
                'destiantion_port': 8500,
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
        def on_publish(client, userdata, mid):
            print(">> SENT MQTT: {}".format(mid))

        @staticmethod
        def on_message(client, userdata, msg):
            temp = msg.payload.decode()
            print("Message arrived:")
            print(temp)
            if temp == "##START##":
                self.emit()
            elif msg.payload.decode() == "##STOP##":
                self.stop_emit()
            else:
                print('Message arrived')
                data_json = json.loads(temp)
                data_json = json.loads(data_json)
                self.agregate(data_json)

        @staticmethod
        def on_connect(mqtt_client, userdata, flags, rc):
            for topic in [self._config['mqtt']['recive_topic']]:
                mqtt_client.subscribe(topic)
            print('Connection esablished with code: ' + str(rc))

        @staticmethod
        def reg_on_publish(client, userdata, mid):
            print('Attempting to register...')

        # check if admin panel registered controler (checks if msg equals self.uuid)
        @staticmethod
        def reg_on_message(client, userdata, msg):
            client.disconnect()
            client.loop_stop()
            content = msg.payload.decode()
            if str(self.uuid) == content:
                self.registered = True
                print('Attempt sucessfull')

        # Routing
        @self.server.route('/', methods=['post'])
        def intercept():
            print("Intercepted")
            data_json = request.get_json()
            self.agregate(data_json)
            return jsonify({'status': 'Ok'})

        @self.server.route('/start', methods=['post', 'get'])
        def start():
            self.active = True
            self.emit()
            return jsonify({'status': 'START'})

        @self.server.route('/stop', methods=['post', ' get'])
        def stop():
            print("STOP")
            self.active = False
            self.stop_emit()
            return jsonify({'status': 'STOP'})
            
        @self.server.route('/info')
        def info():
            return jsonify({'config': self._config})

        @self.server.route('/status', methods=['post', ' get'])
        def status():
            return jsonify({'status': self.active, 'sending': self.sending})

        @self.server.route('/config', methods=['GET', 'POST'])
        def config():
            content = request.get_json()
            self._config = content
            return jsonify({'config_sucess': True})

        self.register_agent.loop_start()
        self.register_agent.on_publish=reg_on_publish

        self.register_agent.on_message=reg_on_message
        self.register_agent.connect(self._config['mqtt']['broker'], int(self._config['mqtt']['broker_port']))
        self.register_agent.subscribe(self.register_topic)


        while not self.registered:
            self.register_agent.subscribe(self.register_topic)
            self.register()

        self.mqtt_client = mqtt.Client(client_id=str(self.uuid), transport='websockets')
        self.mqtt_client.loop_start()
        self.mqtt_client.on_publish=on_publish
        self.mqtt_client.on_connect=on_connect
        self.mqtt_client.on_message=on_message
        self.mqtt_client.connect(self._config['mqtt']['broker'], int(self._config['mqtt']['broker_port']))
        self.server.run(port=self.port)

    def agregate(self, data_json:dict) -> None:
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

        if self.memory_queue.shape[0] == int(self._config['pack_size']):
            self.emit()
        return None

    def selection(self, selection: str, group_function: str) -> pd.DataFrame:
        temp_memory = self.memory_queue.copy()
        temp_memory[selection] = temp_memory[selection].astype('float')
        if group_function != '':
            temp_memory = eval("temp_memory." + str(group_function) + "(axis=0, numeric_only=True).to_frame().T")
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
            self.memory_queue.drop(index=y, inplace=True)
            t_str = t_str[:-1:]
            t_str += '}'
            pack.put(t_str)
        return pack

    def register(self):
        self.register_agent.publish('agreg_register_8678855', json.dumps({"uuid": str(self.uuid), "config": self._config, "ip": self.ip, "port": self.port}))
        time.sleep(2)

    def http(self):
        self.sending = True
        data = self.package()
        while self.sending:
            if data.qsize() != 0:
                x = data.get()
                print(x)
                print(type(x))
                x = x[x.index(':')+2:-2]
                x = json.loads(x)
                print(x)
                print(type(x))
                pload = json.dumps(x)
                content = 'http://' + self._config['http']['destiantion'] +":"+ str(self._config['http']['destiantion_port']) + str(self._config['http']['destiantion_path'])
                headers = {
                  'Content-Type': 'application/json'
                }
                r = requests.request('POST', content, data=pload, headers=headers)
                print(">> SENT HTTP {}: {} | {}".format(r.status_code, content, json.dumps(pload)))
                time.sleep(int(self._config['frequency']))
                self.sending = True
            else:
                self.sending = False
                break

    def mqtt(self):
        self.sending = True
        data = self.package()
        while self.sending:
            if data.qsize() != 0:
                content = (self._config['mqtt']['send_topic'], json.dumps({'data': data.get()}))
                self.mqtt_client.publish(content[0], content[1])
                time.sleep(self._config['frequency'])
                self.sending = True
            else:
                self.sending = False
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
