from flask import Flask, request

app = Flask(__name__)
@app.route('/', methods=['POST'])
def result():
    print(request.json) # should display 'bar'
    return 'Received !' # response to your request.

if __name__ == "__main__":
    app.run(debug=True)