import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time
import uuid

# Function to read the configuration from the JSON file
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# Callback to confirm message delivery
def on_publish(client, userdata, mid):
    print("Message published with QoS 0 and confirmed by the broker")

# Load the configuration
config = load_config('config_connect.json')

null = None  # Use None instead of null

while True:
    # Get the current system time in the required format
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

    # Generate a random TransactionID
    transaction_id = str(uuid.uuid4())

    message = {
        "MessageName": "CFX.Production.Processing.UnitsProcessed",
        "Version": "1.7",
        "TimeStamp": current_time,
        "UniqueID": "d--0000-0000-0012-006d",
        "Source": "d--0000-0000-0012-006d",
        "Target": null,
        "RequestID": null,
        "MessageBody": {
            "$type": "CFX.Production.Processing.UnitsProcessed, CFX",
            "TransactionID": transaction_id,
            "OverallREsult": "Pass",
            "RecipeName": "Recipe_01", 
            "CommonProcessData": 
                   {
                    "$type": "CFX.Structures.ProccessData, CFX", 
                    "PersonalizedUnits": 
                        [
                            {
                                "NAME": "Voltage_1", 
                                "UNIT": "None", 
                                "Value": 706.0, 
                                "Unit": null, 
                                "HILIM": null, 
                                "LOLIM": null, 
                                "STATUS": "PASS", 
                                "RULE": "EQ", 
                            "TARGET": null
                            }, 
                            {
                             "NAME": "Current_1", 
                             "UNIT": "None", 
                             "Value": 2e-06, 
                             "Unit": null, 
                             "HILIM": null, 
                             "LOLIM": null, 
                             "STATUS": "PASS", 
                             "RULE": "EQ", 
                             "TARGET": null
                            }, 
                            {
                              "NAME": "Voltage_2", 
                              "UNIT": "None", 
                              "Value": 706.0, 
                              "Unit": null, 
                              "HILIM": null, 
                              "LOLIM": null, 
                              "STATUS": "PASS", 
                              "RULE": "EQ", 
                              "TARGET": null
                            }, 
                            {
                               "NAME": "Current_2", 
                               "UNIT": "None", 
                               "Value": 2e-06, 
                               "Unit": null, 
                               "HILIM": null, 
                               "LOLIM": null, 
                               "STATUS": "PASS", 
                               "RULE": "EQ", 
                               "TARGET": null
                            }
                        ]
                    }, 
            "Metadata": 
                    {
                        "building": "B16", 
                        "device": "01", 
                        "area_name": "Ciena", 
                        "org": "Flex", 
                        "line_name": "SMT_Ciena_line6", 
                        "site_name": "Guadalajara_North", 
                        "station_name": "Map", 
                        "Process_type": "screwing", 
                        "machine_name": "Map_screwing_01", 
                        "Created_by": "GA",
                        "previus_status": "Ready", 
                        "time_last_status": "0:00:20"
                    }, 

            }
    }

    # Convert the message to a JSON string
    message_json = json.dumps(message)

    # Create an instance of the MQTT client
    client = mqtt.Client()

    # Set username and password
    client.username_pw_set(config['username'], config['password'])

    # Assign the publish callback
    client.on_publish = on_publish

    # Connect to the MQTT broker
    client.connect(config['broker'], config['port'])

    # Publish a message to the specified topic with QoS 1
    client.publish(config['topic'], message_json, qos=1)

    # Disconnect from the broker
    client.disconnect()

    print(f"Message '{message_json}' sent to topic '{config['topic']}' on broker '{config['broker']}:{config['port']}' with QoS 1")

    # Wait 40 seconds before sending the next message
    time.sleep(40)

