# https://medium.com/better-programming/introduction-to-message-queue-with-rabbitmq-python-639e397cb668
# producer.py
# This script will publish MQ message to my_exchange MQ exchange

import pika
import os
from dotenv import load_dotenv

load_dotenv()

IP_ELASTICA_RABBITMQ = os.getenv('IP_ELASTICA_RABBITMQ')
USUARIO_RABBITMQ = os.getenv('USUARIO_RABBITMQ')
PASSWORD_RABBITMQ = os.getenv('PASSWORD_RABBITMQ')

connection = pika.BlockingConnection(pika.ConnectionParameters(IP_ELASTICA_RABBITMQ, 5672, '/', pika.PlainCredentials(USUARIO_RABBITMQ, PASSWORD_RABBITMQ)))
channel = connection.channel()

channel.basic_publish(exchange='my_exchange', routing_key='', body='GuerraTeAmo')

connection.close()