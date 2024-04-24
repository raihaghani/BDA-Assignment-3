//Logs received messages 
from kafka import KafkaConsumer
import json

def create_consumer():
    consumer = KafkaConsumer(
        'my_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='group1',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def consume_data(consumer):
    for message in consumer:
        data = message.value
        print(f"Consumer 1 received: {data}")

if __name__ == "__main__":
    consumer = create_consumer()
    consume_data(consumer)
