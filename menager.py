from flask import Flask, request
import subprocess
import random
import uuid
import json
import time


x = {
    "data": {
        "source": "./datasets/biomechanical.csv",
        "channel": "HTTP",
        "frequency": 1
    },
    "MQTT": {
        "broker": "test.mosquitto.org",
        "port": 1883,
        "topic": "testiot439"
    },
    "HTTP": {
        "host": "127.0.0.1",
        "port": 5000
    }
}


class Menager:
    def __init__(self) -> None:
        self.app = Flask(__name__)
        self.register = {}
        self._config = ''

        @self.app.route('/<name>/start', methods=['post'])
        # TODO: Posibble error 
        def start(name):
            self._config = request.get_json()
            self._config = json.dumps(self._config)
            self.start_generator(name, self._config)
            return """Sent"""
                # self._config = {
                # 'data': {'source': request.form.get('source'), 'channel': request.form.get('channel'), 'frequency': request.form.get('frequency')},
                # 'MQTT': {'broker': request.form.get('broker'), 'port': request.form.get('port'), 'topic': request.form.get('topic')},
                # 'HTTP': {'host': request.form.get('host'), 'port': request.form.get('port')},
                # }
                    # self.load()
                    # self.send()

        @self.app.route('/<name>/stop', methods=['post'])
        # TODO: Posibble error 
        def stop(name):
            self.start_generator(name)
            return """Halted"""

    def start_generator(self, id:str, config:dict):
        # temp = subprocess.run(['python', 'generator.py', '--config', json.dumps(config)])
        try:
            temp = subprocess.Popen(['python', 'generator.py', '--config', json.dumps(config)], stdout=subprocess.PIPE)
        except:
            print("Error")
        self.register.update({id: temp})

    def stop_generator(self, name:str):
        self.register[name].kill()

    def update_generator(self):
        pass

    def set_up(self, port:int):
        self.app.run(port=port, debug=True)



if __name__ == "__main__":
    p = Menager()
    p.set_up(8000)

