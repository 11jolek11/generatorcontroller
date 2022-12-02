from flask import Flask, request, jsonify
import subprocess
import json
import paho.mqtt.client as mqtt
import time
import uuid



class Controller:
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()
        self.ip = '127.0.0.1'
        self.port = 8080
        self.app = Flask(__name__)
        self.register = {}
        self._config = ''
        self.auto_register()
        @self.app.route('/<id>/start', methods=['post']) 
        def start(id):
            self._config = request.get_json()
            self._config = json.dumps(self._config)
            self.start_generator(id, self._config)
            return jsonify({'req_status': str(id) + ' started'})

        @self.app.route('/<id>/stop', methods=['post'])
        def stop(id):
            self.stop_generator(id)
            return jsonify({'req_status': str(id) + ' stopped'})

    def start_generator(self, id:str, config:str):
        if id in self.register.keys():
            self.register[id].kill()
            print('Updating...')

        temp = None
        temp = subprocess.Popen(['python', 'generator.py', '--config', config])
        self.register.update({id: temp, 'config': config})

    def stop_generator(self, id:str):
        print("\033[93m* Generator deactivated\033[0m")
        self.register[id].kill()
        del self.register[id]

    def status_generator(self, id:str):
        if id in self.register.keys():
            return jsonify({'id': id, 'status_code': 1, 'status': 'up'})
        else:
            return jsonify({'id': id, 'status_code': 0, 'status': 'down'})

    def set_up(self, port:int):
        self.port =port
        self.app.run(port=port, debug=True)

    def auto_register(self):
        transaction_completed = False

        def reg_on_connect(client, userdata, flags, rc):
            print('Connected to register with result code: ' + str(rc))

        def reg_on_publish(client, userdata, mid):
            print('Attempting to register...')

        # check if admin panel registered controler (checks if msg equals self.uuid)
        def reg_on_message(client, userdata, msg):
            content = msg.payload.decode()
            if str(self.uuid) == content:
                print('Attempt sucessfull')
                transaction_completed = True
        
        client = mqtt.Client()
        client.on_connect = reg_on_connect
        client.on_publish = reg_on_publish
        client.on_message = reg_on_message

        client.connect("test.mosquitto.org", 1883)

        while not transaction_completed:
            avaible_generators = str(list(self.register.keys())).replace('[', '').replace(']', '').replace(',', '#')
            message = str(self.uuid) + '$' + self.ip + ':' + str(self.port) + '@' + avaible_generators
            time.sleep(3)
            client.publish('waitroom458', message)

        client.disconnect()
            


if __name__ == "__main__":
    p = Controller()
    p.set_up(8000)
