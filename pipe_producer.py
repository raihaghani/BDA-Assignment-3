from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def produce_data(file_path):
  
    with open(file_path, 'r') as file:
        data = json.load(file)
    =
    for item in data:
        producer.send('my_topic', value=item)
        print(f"Produced: {item}")
        producer.flush()

if __name__ == "__main__":
    file_path = 'PreProcessedData.json'  
    produce_data(file_path)
