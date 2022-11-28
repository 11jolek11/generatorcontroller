from flask import Flask, request
import subprocess
import random
import uuid
import json
import time



class Controller:
    def __init__(self) -> None:
        self.app = Flask(__name__)
        self.register = {}
        self._config = ''
        # TODO: Add UUID instead of name
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
        def stop(name):
            self.stop_generator(name)
            return """Halted"""

    def start_generator(self, id:str, config:dict):
        # TODO: consider adding restriction to one generator per menager instance
        temp = 0
        # str_config= json.dumps(config)
        # print(type(config))
        # print(config)

        # temp = subprocess.run(['python', 'generator.py', '--config', json.dumps(config)])
        temp = subprocess.Popen(['python', 'generator.py', '--config', config])

        self.register.update({id: temp})

    def stop_generator(self, name:str):
        print("\033[93m* Generator deactivated\033[0m")
        self.register[name].kill()

    def set_up(self, port:int):
        self.app.run(port=port, debug=True)



if __name__ == "__main__":
    p = Controller()
    p.set_up(8000)

