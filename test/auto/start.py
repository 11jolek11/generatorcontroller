import requests
import json

url = "http://127.0.0.1:8000/jack/start"

payload = json.dumps({
  "data": {
    "source": "./datasets/biomechanical.csv",
    "channel": "MQTT",
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
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
