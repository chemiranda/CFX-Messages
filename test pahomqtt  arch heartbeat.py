import paho.mqtt.client as mqtt
import json
from datetime import datetime

import time


# Función para leer la configuración desde el archivo JSON
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# Callback para confirmar la entrega del mensaje
def on_publish(client, userdata, mid):
    print("Mensaje publicado con QoS 1 y confirmado por el broker")

# Callback para manejar la conexión
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connection to Broker stablished")
    elif rc == 4:
        print("Error: User or Password incorrect")
    else:
        print(f"Error Connection: {rc}")

# Función para crear el mensaje
def create_message():
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
    message = {
        "MessageName": "CFX.Heartbeat",
        "Version": "1.7",
        "TimeStamp": current_time,
        "UniqueID": "d--0000-0000-0012-0f6a",
        "Source": "d--0000-0000-0012-0f6a",
        "Target": None,
        "RequestID": None,
        "MessageBody": {
            "$type": "CFX.Heartbeat, CFX",
            "CFXHandle": None,
            "HeartbeatFrequency": "00:05:00",
            "ActiveFaults": [],
            "ActiveRecipes": [],
            "Metadata": {
                "building": "3501_Sync_Backend",
                "device": "02",
                "area_name": "3501_Sync_Backend",
                "org": "Flex",
                "line_name": "Sync_4",
                "site_name": "Coopersville",
                "station_name": "Map",
                "Process_type": "PicknPlace",
                "machine_name": "MAP-2",
                "Created_by": "GA"
            },   
        }
    }
    return json.dumps(message)

# Cargar la configuración
config = load_config('config_connect2.json')

# Crear una instancia del cliente MQTT
client = mqtt.Client()

# Configurar usuario y contraseña
client.username_pw_set(config['username'], config['password'])

# Asignar los callbacks
client.on_publish = on_publish
client.on_connect = on_connect
# Conectar al broker MQTT
client.connect(config['broker'], config['port'])

# Iniciar el loop para procesar los callbacks
client.loop_start()

# Publicar el mensaje de forma cíclica cada 5 minutos
try:
    while True:
        message_json = create_message()
        client.publish(config['topic'], message_json, qos=1)
        print(f"Mensaje '{message_json}' enviado al tema '{config['topic']}' en el broker '{config['broker']}:{config['port']}' con QoS 1")
        time.sleep(300)  # Esperar 5 minutos (300 segundos) antes de enviar el siguiente mensaje
except KeyboardInterrupt:
    print("Interrupción del usuario. Desconectando...")
finally:
    client.loop_stop()
    client.disconnect()