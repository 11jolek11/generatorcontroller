from flask import Flask, request
import subprocess
import json



class Controller:
    def __init__(self) -> None:
        self.app = Flask(__name__)
        self.register = {}
        self._config = ''
        # TODO: Add UUID instead of id
        @self.app.route('/<id>/start', methods=['post']) 
        def start(id):
            self._config = request.get_json()
            self._config = json.dumps(self._config)
            self.start_generator(id, self._config)
            # TODO: change to JSON
            return """Sent"""

        @self.app.route('/<id>/stop', methods=['post'])
        def stop(id):
            self.stop_generator(id)
            # TODO: change to JSON
            return """Halted"""

    def start_generator(self, id:str, config:str):
        # TODO: dodaj funkcje która będzie zmieniać w wartości w czasie działania generatora
        # TODO: consider adding restriction to one generator per Controler instance
        # if id in self.register.keys():
        #     self.register[id].communicate(input=config)
        #     print('Updating...')


        if id in self.register.keys():
            self.register[id].kill()
            print('Updating...')

        temp = None
        temp = subprocess.Popen(['python', 'generator.py', '--config', config])
        self.register.update({id: temp})

    def stop_generator(self, name:str):
        print("\033[93m* Generator deactivated\033[0m")
        self.register[name].kill()
        del self.register[name]

    def set_up(self, port:int):
        self.app.run(port=port, debug=True)



if __name__ == "__main__":
    p = Controller()
    p.set_up(8000)
