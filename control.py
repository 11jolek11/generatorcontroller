from flask import Flask, request, jsonify
import subprocess
import json



class Controller:
    def __init__(self) -> None:
        self.app = Flask(__name__)
        self.register = {}
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
        self.app.run(port=port, debug=True)



if __name__ == "__main__":
    p = Controller()
    p.set_up(8000)
