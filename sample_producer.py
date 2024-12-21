import time
import json
import random
from confluent_kafka import Producer

# Kafka configuration
bootstrap_servers = '10.0.0.30:9092'
topic_name = 'my_topic1'

# SASL authentication configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'admin-secret'
}

# Create a Kafka producer
producer = Producer(conf)

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
       Triggered by poll() or flush()."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_message(producer, topic, message):
    try:
        producer.produce(topic, value=message, callback=delivery_report)
        producer.poll(0)
    except Exception as e:
        print(f'Failed to send message: {e}')

if __name__ == '__main__':
    try:
        while True:
            sensor_data = [
                {
                    "device_id": "fa3c3400-1ce6-11ef-9039-4ff2c1f6b217",
                    "device_name": "Device A",
                    "name": "humidity",
                    "type": "rh",
                    "reading": random.uniform(30.0, 60.0),
                    "status": "SEVERE",
                    "readingType": "%"
                },
                {
                    "device_id": "fa3c3400-1ce6-11ef-9039-4ff2c1f6b217",
                    "device_name": "Device A",
                    "name": "temperature",
                    "type": "temp",
                    "reading": random.uniform(20.0, 30.0),
                    "status": "NORMAL",
                    "readingType": "f"
                }
            ]
            message = json.dumps(sensor_data)
            print(f'Started pushing data into topic {topic_name}: {message}')
            send_message(producer, topic_name, message)
            # producer.flush()  # Ensure all messages are sent
            print('Pushed successfully')
            time.sleep(3)
    except KeyboardInterrupt:
        print('Process interrupted')
    finally:
        try:
            producer.flush()  # Ensure all messages are sent
        except Exception as e:
            print(f'Failed to flush producer: {e}')
