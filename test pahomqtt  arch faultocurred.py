import paho.mqtt.client as mqtt
import json
null="null"

#broker MQTT
broker = "archfx-am-1.flex.com"
port = 1883
topic = "flex-smt.raw.ps--0000-0007.cfx.flex-v1.d--0000-0000-0012-0f6a"
username = "cfx-guadalajara-n"
password = "347277874783"

message = {
    "MessageName": "CFX.ResourcePerformance.FaultOccurred",
    "Version": "1.7",
    "TimeStamp": "2025-06-12T08:40:02-06:00",
    "UniqueID": "d--0000-0000-0012-0f6a",
    "Source": "d--0000-0000-0012-0f6a",
    "Target": null,
    "RequestID": null,
    "MessageBody": {
        "$type": "CFX. ResourcePerformance.FaultOccurred, CFX",
        "Fault": {
            "TransactionID": "e53eb476-da16-4f22-8a52-ac40e1754ff8",
            "Cause": "LoadError",
            "Severity": "Information",
            "FaultCode":"1001",
            "FaultOccurrenceId": "fda6276c-e4ab-4796-9f76-ac369d7b8aaa",
            "Lane": 1,
            "Stage": {
            "StageSequence": 0,
            "StageName": "Stage0",
            "StageType": "Work",
            },
        "SideLocation": "Unknown",
        "AccessType": "Unknown",
        "Description": "SafeDoor open",
        "DescriptionTranslations": {
        "bool": "false"
            },
        "OccurredAt": "2025-06-12T08:40:02-06:00",
        "DueDateTime": null
        }      
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

# Convertir el mensaje a una cadena JSON
message_json = json.dumps(message)

# Crear una instancia del cliente MQTT
client = mqtt.Client()

# Configurar usuario y contraseña
client.username_pw_set(username, password)

# Conectar al broker MQTT
client.connect(broker, port)

# Publicar un mensaje en el tema especificado con QoS 1
client.publish(topic, message_json, qos=0)

# Desconectar del broker
client.disconnect()

print(f"Mensaje '{message_json}' enviado al tema '{topic}' en el broker '{broker}:{port}' con QoS 0")
