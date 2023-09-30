import paho.mqtt.client as mqtt
import time
import random
import json


# Función para simular una base de datos y generar datos aleatorios
def obtener_datos_simulados():
    # En este ejemplo, generamos un valor de temperatura aleatorio entre 20 y 30 grados Celsius.
    temperatura = round(random.uniform(-20, 12), 2)

    data = {
        "Entrada": "Prueba 1",
        "Temperatura": temperatura
    }

    # Convertir el diccionario en una cadena JSON
    temperatura_format = json.dumps(data)

    return temperatura_format

# Función para enviar datos al servidor MQTT
def enviar_datos_mqtt():
    # Configurar el cliente MQTT
    client = mqtt.Client()
    client.connect("broker.mqtt-dashboard.com", port=1883)

    # Bucle para generar y enviar datos cada 5 segundos
    while True:
        # Obtener datos simulados
        temperatura = obtener_datos_simulados()

        # Publicar los datos en un tema MQTT
        client.publish("equipoPATO", str(temperatura))

        # Imprimir los datos enviados
        print(f"Temperatura enviada: {temperatura}°C")

        # Esperar 5 segundos antes de enviar más datos
        time.sleep(5)

# Ejecutar la función para enviar datos MQTT
enviar_datos_mqtt()
