from flask import Flask, request, jsonify
import subprocess
import json
import paho.mqtt.client as mqtt
import time
import uuid
from flask_cors import CORS




class Controller:
    def __init__(self) -> None:
        self.uuid = uuid.uuid4()
        # self.uuid = 1
        self.ip = '127.0.0.1'
        self.port = 8080
        self.register = {}
        self.client = mqtt.Client(client_id='11jolek11', transport='websockets')

        def reg_on_connect(client, userdata, flags, rc):
            print('Connected to register with result code: ' + str(rc))
            client.subscribe("transaction_channel")
        def reg_on_publish(client, userdata, mid):
            print('Attempting to register...')
        # check if admin panel registered controler (checks if msg equals self.uuid)
        def reg_on_message(client, userdata, msg):
            client.disconnect()
            content = msg.payload.decode()
            if str(self.uuid) == content:
                print('Attempt sucessfull')
            client.disconnect()

        self.client.on_connect = reg_on_connect
        self.client.on_publish = reg_on_publish
        self.client.on_message = reg_on_message
        # self.client.connect("test.mosquitto.org", 8080)

        # self.auto_register()
        self.app = Flask(__name__)
        CORS(self.app)
        self._config = ''
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

        @self.app.route('/<id>/status', methods=['post'])
        def status(id):
            return self.status_generator(id=id)

    def start_generator(self, id:str, config:str):
        if id in self.register.keys():
            self.register[id].kill()
            print('Updating...')

        temp = None
        temp = subprocess.Popen(['python', 'generator.py', '--config', config])
        self.register.update({id: temp, 'config': config})
        avaible_generator = json.dumps({
            # ID generatora
            'name': id,
            # IP interfaceu
            'ip': self.ip,
            # Port interfaceu
            'port': self.port,
            # Klucz interfejsu
            'uuid': str(self.uuid),
        })
        self.client.connect("test.mosquitto.org", 8080)
        self.client.subscribe("transaction_channel")
        self.client.publish('waitroom459', avaible_generator)
        self.client.loop_forever(timeout=2)
        self.client.disconnect()
        return None
        

    def stop_generator(self, id:str):
        print("\033[93m* Generator deactivated\033[0m")
        self.register[id].kill()
        del self.register[id]

    def status_generator(self, id:str):
        if id in self.register.keys():
            return jsonify({'id': id, 'status_code': 1, 'status': 'up'})
        else:
            return jsonify({'id': id, 'status_code': 0, 'status': 'down'})

    def set_up(self, port:int) -> None:
        self.port = port
        self.app.run(port=port, debug=True)
    
    # def auto_register(self):
    #     # FIXME: auto_register runs two times why!!??
    #     def reg_on_connect(client, userdata, flags, rc):
    #         print('Connected to register with result code: ' + str(rc))
    #         client.subscribe("transaction_channel")

    #     def reg_on_publish(client, userdata, mid):
    #         print('Attempting to register...')

    #     # check if admin panel registered controler (checks if msg equals self.uuid)
    #     def reg_on_message(client, userdata, msg):
    #         client.disconnect()
    #         content = msg.payload.decode()
    #         if str(self.uuid) == content:
    #             print('Attempt sucessfull')
    #         client.disconnect()
        
    #     # client = mqtt.Client(client_id='11jolek11')
    #     client = mqtt.Client(client_id='11jolek11', transport='websockets')
    #     client.on_connect = reg_on_connect
    #     client.on_publish = reg_on_publish
    #     client.on_message = reg_on_message

    #     # client.connect("test.mosquitto.org", 1883)
    #     client.connect("test.mosquitto.org", 8080)

    #     avaible_generators = {
    #         'ip': self.ip,
    #         'port': self.port,
    #         'uuid': str(self.uuid),
    #         'generators': list(self.register.keys())
    #     }


    #     message = json.dumps(avaible_generators)
    #     # avaible_generators = str(list(self.register.keys())).replace('[', '').replace(']', '').replace(',', '#')
    #     # message = str(self.uuid) + '$' + self.ip + ':' + str(self.port) + '@' + avaible_generators
    #     client.subscribe("transaction_channel")
    #     client.publish('waitroom459', message)
    #     client.loop_forever()
    #     client.disconnect()
    #     return None
            


if __name__ == "__main__":
    p = Controller()
    p.set_up(7000)
