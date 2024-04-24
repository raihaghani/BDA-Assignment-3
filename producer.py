from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def send_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)  # Load the whole file as a JSON array
        for product in data:
            producer.send('amazon_products', value=product)
            time.sleep(2.0)  # Simulate real-time data feed
            print(f"Produced: {product['asin']}")

# File path to your JSON data
file_path = '/home/ebraheem/Documents/BDA3/PreProcessedData.json'  # Adjust this path as needed
send_data(file_path)
