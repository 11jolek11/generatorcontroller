import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
      # The callback for when 
    # the client connects to the broker 
    print("Connected with result code {0}".format(str(rc)))
    client.subscribe("testiot439")
    # client.subscribe("waitroom459")
    # Print result of connection attempt client.subscribe("digitest/test1")  
    # Subscribe to the topic “digitest/test1”, receive any messages 
    # published on it


def on_message(client, userdata, msg):  # The callback for when a PUBLISH 
    # message is received from the server. 
    print("Message received-> " + msg.topic + " " + str(msg.payload.decode()))  # Print a received msg


client = mqtt.Client()  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect('test.mosquitto.org', 1883)
client.loop_forever()  # Start networking daemon