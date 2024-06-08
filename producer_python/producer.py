from confluent_kafka import Producer
from flask import Flask, request, jsonify
import random
import os

app = Flask(__name__)
producer = None

def connect_kafka_producer():
    conf = {
        'bootstrap.servers': os.getenv("SERVER_KAFKA", "flask_server:8080"),
        'api.version.request': True
    }
    producer = Producer(conf)
    return producer

def delivery_report(err, msg):
    if err is not None:
        print(f'Falha ao entregar a mensagem: {err}')
    else:
        print(f'Mensagem enviada para {msg.topic()} [{msg.partition()}]')

@app.route('/produce', methods=['POST'])
def produce_message():
    data = request.get_json()
    if 'message' not in data:
        return jsonify({'error': 'A mensagem é obrigatória'}), 400
    
    global producer
    if producer is None:
        producer = connect_kafka_producer()

    producer.produce("kafka-python-topic", value=data['message'], callback=delivery_report)
    producer.flush()
    return jsonify({'message': 'Mensagem enviada para o Kafka'}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
