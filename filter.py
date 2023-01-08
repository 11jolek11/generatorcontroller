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



class Filter():
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()

        self.ip = '127.0.0.1'
        self.port = 8500

        self.register_channel = "mqtt"
        self.register_topic = "goblin-5644"

        self.server = Flask(str(self.uuid))
        CORS(self.server)

        self.active = True

        # This flag should be on False in production env
        self.registered = False
        # on true now because debugging purposes
        # self.registered = True

        self.memory_queue = pd.DataFrame()

        self.register_agent = mqtt.Client(client_id=str(self.uuid), transport='websockets')

        self.last_data = [None]

        self.sending = False
        # https://www.listendata.com/2019/07/how-to-filter-pandas-dataframe.html
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
                'send_topic': 'send_5948',
                'recive_topic': 'recive_4543',
            },
            'constraints': {
                'query': 'GMSL',
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
                data_json = json.loads(temp)
                data_json = json.loads(data_json)
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
            data_json = request.json
            self.agregate(data_json)
            return jsonify({'status': 'Ok'})

        @self.server.route('/start', methods=['post', 'get'])
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
            return jsonify({'config': self._config})

        @self.server.route('/status', methods=['post', ' get'])
        def status():
            return jsonify({'status': self.active, 'sending': self.sending})


        @self.server.route('/config', methods=['GET', 'POST'])
        def config():
            content = request.get_json()
            print(content)
            self._config = content
            return jsonify({'config_sucess': True})

        self.register_agent.loop_start()
        self.register_agent.on_connect=on_connect
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
        self.last_data.append(data_json)
        df = json_normalize(data_json)

        if self.memory_queue.empty:
            self.memory_queue = df
            print("if inside agregte()")
        else:
            self.memory_queue= pd.concat([self.memory_queue, df], ignore_index=True)
        print("bbb")
        if self.memory_queue.shape[0] == 1:
            print("eee")
            self.emit()
        print("aaa")
        return None

    def selection(self, query: str, selection: dict) -> pd.DataFrame:
        print('Selecting...')
        temp_memory = self.memory_queue.copy()
        print("########")
        print(temp_memory)
        # TODO: extra: try query function
        x = temp_memory[[query]]
        print(x)
        return x

    def package(self):
        pack = Queue()
        if self._config['constraints']['query'] != '':
            data = self.selection(self._config['constraints']['query'], self._config['constraints']['selection'])
        else:
            data = self.memory_queue.copy()

        print("Series problem")
        print(self.memory_queue)
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
        self.register_agent.publish('filter_register_8678855', json.dumps({"uuid": str(self.uuid), "config": self._config, "ip": self.ip, "port": self.port}))
        time.sleep(2)

    def http(self):
        self.sending = True
        data = self.package()
        while self.sending:
            if data.qsize() != 0:
                print('if')
                pload = json.dumps({'data': data.get()})
                content = 'http://' + self._config['http']['destiantion'] +":"+ str(self._config['http']['destiantion_port']) + str(self._config['http']['destiantion_path'])
                headers = {
                  'Content-Type': 'application/json'
                }
                r = requests.request('POST', content, data=pload, headers=headers)
                print(">> SENT HTTP {}: {} | {}".format(r.status_code, content, pload))
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
    p = Filter()
