import paho.mqtt.client as mqtt
import json
from datetime import datetime

# Función para leer la configuración desde el archivo JSON
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# Callback para confirmar la entrega del mensaje
def on_publish(client, userdata, mid):
    print("Mensaje publicado con QoS 1 y confirmado por el broker")

# Cargar la configuración
config = load_config('config_connect.json')

null = None  # Usar None en lugar de null

# Obtener la hora actual del sistema en el formato requerido
current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

message =  {
    "MessageName": "CFX. ResourcePerformance.FaultCleared",
    "Version": "1.7",
    "TimeStamp": current_time,
    "UniqueID": "d--0000-0000-0012-0f6a",
    "Source": "d--0000-0000-0012-0f6a",
    "Target": null,
    "RequestID": null,
    "MessageBody": {
            "$type": "CFX. ResourcePerformance. FaultCleared, CFX",        
            "FaultOccurrenceId": "fda6276c-e4ab-4796-9f76-ac369d7b8aae",
            "Operator" : {
            "OperatorIdentifier": "BADGE4486",
            "ActorType": "Robot",
            "LastName": "AB",
            "FirstName": "CD",
            "LoginName": "ABCD@123"
                        },  
            "ClearedAt": "2025-06-11T22:38:02-06:00",
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
client.username_pw_set(config['username'], config['password'])

# Asignar el callback de publicación
client.on_publish = on_publish

# Conectar al broker MQTT
client.connect(config['broker'], config['port'])

# Publicar un mensaje en el tema especificado con QoS 1
client.publish(config['topic'], message_json, qos=1)

# Desconectar del broker
client.disconnect()

print(f"Mensaje '{message_json}' enviado al tema '{config['topic']}' en el broker '{config['broker']}:{config['port']}' con QoS 1")
