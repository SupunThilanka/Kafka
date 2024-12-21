import json
from confluent_kafka import Consumer

# Kafka configuration
config2 = {
    "bootstrap.servers": "52.21.129.119:9092",
    'group.id': 'my-group-id',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'admin-secret'
}

# Create a Kafka consumer
consumer = Consumer(config2)

# List topics and print them
topics = consumer.list_topics()
for topic in topics.topics:
    print(topic)

# Subscribe to the topic of interest
topics_of_interest = ["test"]
consumer.subscribe(topics_of_interest)

message_records = []

def update_message_records(new_message):
    # Check if the message with the same "name" already exists
    for record in message_records:
        if record['name'] == new_message['name']:
            # Update the existing record
            record['reading'] = new_message['reading']
            record['status'] = new_message['status']
            record['readingType'] = new_message['readingType']
            return

    # If no existing record is found, add the new message to the list
    message_records.append(new_message)

while True:
    try:
        message = consumer.poll(1.0)
        if message is None:
            print("No message Found")
            continue
        if message.error():
            print((message.error()))
        else:
            message_value = message.value().decode('utf-8')
            try:
                message_json = json.loads(message_value)
                # update_message_records(message_json)
                print(message_json)
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON message {e}")
    except Exception as e:
        print(e)
        print("ERROR")
        break
