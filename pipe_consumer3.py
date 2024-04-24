//hypothetical transformation on the data by adding a new key-value pair to each JSON object received and prints the modified data.
from kafka import KafkaConsumer
import json

def create_consumer():
    consumer = KafkaConsumer(
        'my_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='group3',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def consume_data(consumer):
    for message in consumer:
        data = message.value
        # Perform a transformation: Add a new key 'processed' with value 'true'
        data['processed'] = True
        print(f"Consumer 3 transformed data: {data}")

if __name__ == "__main__":
    consumer = create_consumer()
    consume_data(consumer)
