import requests

url = "http://127.0.0.1:8000/jack/stop"

payload={}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
