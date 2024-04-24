//counts the number of messages received and prints the count every time a new message is processed.
from kafka import KafkaConsumer
import json

def create_consumer():
    consumer = KafkaConsumer(
        'my_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='group2',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def consume_data(consumer):
    count = 0
    for message in consumer:
        data = message.value
        count += 1
        print(f"Consumer 2 processed message #{count}: {data}")

if __name__ == "__main__":
    consumer = create_consumer()
    consume_data(consumer)
