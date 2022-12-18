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
    # TODO cd. 
    # Idea 1: kiedy przesyłamy jsona do agragatora to tworzymy z nazw kolumn hash który identyfikuje zbiór danych
    # z DF self.memory_queue robimy słownik {hash: zbior danych}
    # Idea 2: w odpowiednim słowniku zapisujemy nazwę źródło i numery kolumn do których zapisujemy
    # Idea 3: Z każdeg generatora zapisujemy do dataframe a dataframe zapisujemy jako wartość słownika {nazwa generatora: dataframe}







    # TODO: Delete selection option - unDone
    # TODO: Implement sending data from time to time (call emitt() from time to time) - Done
    # TODO: Agregator should forget data which it already sent - Done
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()

        self.register_channel = "mqtt"
        self.register_topic = "clock-76467"

        self.server = Flask(str(self.uuid))
        CORS(self.server)

        self.active = True
        self.registered = False

        self.memory_queue = pd.DataFrame()

        self.register_agent = mqtt.Client(client_id=str(self.uuid), transport='websockets')

        self.last_data = [None]

        self._config = {
            'method': 'http',
            'frequency': 0,
            'pack_size': 5,
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
        def on_publish(client, userdata, mid):
            print(">> SENT MQTT: {}".format(mid))

        @staticmethod
        def on_message(client, userdata, msg):
            temp = msg.payload.decode()
            print("Message arrived:")
            print(temp)
            # if msg.payload.decode() == "##START##":
            if temp == "##START##":
                self.emit()
            # elif msg.payload.decode() == "##STOP##":
            elif msg.payload.decode() == "##STOP##":
                self.stop_emit()
            # elif msg.payload.decode() == str(self.uuid) + '$%$':
            # elif temp == str(self.uuid):
            #     print("Registration succesfull")
            else:
                print('Message arrived')
                # data_json = json.loads(msg.payload.decode())
                data_json = json.loads(temp)
                self.agregate(data_json)
                # print(self.memory_queue)

        @staticmethod
        def on_connect(mqtt_client, userdata, flags, rc):
            for topic in [self._config['mqtt']['recive_topic']]:
                mqtt_client.subscribe(topic)
            print('Connection esablished with code: ' + str(rc))
            # mqtt_client.publish(self.register_topic, str(self.uuid))
            # mqtt_client.subscribe("black_4567")

        # Opublikuj swoje uuid jeśli frontend je zwróci (uuid + $%$) to będzie ok
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
        # self.server.run(port=9000, debug=True)
        self.server.run(port=9000)

    def agregate(self, data_json: dict) -> None:
        # Wersja Clean
        # if len(self.last_data) > 1:
        #     if data_json.keys() != self.last_data[-1].keys():
                # Clear DataFrame and prepare for next type of data
                # self.memory_queue = self.memory_queue[0:0]
                # Idea 3 dodajemy nowe kolumny z boku i pojawiają się wartości Nan

        # Wersja z Nan
        self.last_data.append(data_json)
        df = json_normalize(data_json)

        if self.memory_queue.empty:
            self.memory_queue = df
        else:
            self.memory_queue= pd.concat([self.memory_queue, df], ignore_index=True)
        print(self.memory_queue)
        if self.memory_queue.shape[0] == self._config['pack_size']:
            self.emit()
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
        data = self.memory_queue.copy()
        for y in range(data.shape[0]):
            t_str='{'
            # Wersja z Nan
            # FIXME: 
            print(y)
            temp_data = data[y].dropna(axis=1)
            for x in temp_data.tolist():
                t_str += '"' + str(x) + '"'  + ":" + '"' + str(temp_data[x]) + '"' + ","
            self.memory_queue.drop(index=y)
            t_str = t_str[:-1:]
            t_str += '}'
            pack.put(t_str)
        return pack










        # Wersja Clean
        # for y in range(self.memory_queue.shape[0]):
        #     t_str='{'
        #     for x in self.memory_queue.columns.tolist():
        #         t_str += '"' + str(x) + '"'  + ":" + '"' + str(self.memory_queue[x][y]) + '"' + ","
        #     self.memory_queue.drop(index=y)
        #     t_str = t_str[:-1:]
        #     t_str += '}'
        #     pack.put(t_str)
        # return pack

    def register(self):
        self.register_agent.publish('agreg_register_8678855', str(self.uuid))
        time.sleep(10)

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
