from kafka import KafkaConsumer
import json
from textblob import TextBlob
import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["amazon_products"]
collection = db["sentiment_analysis"]

# Initialize Kafka consumer
consumer = KafkaConsumer('amazon_products',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    product = message.value
    description = product.get('description', "")
    sentiment = TextBlob(description).sentiment
    
    # Create document to insert into MongoDB
    document = {
        "asin": product['asin'],
        "sentiment_polarity": sentiment.polarity,
        "sentiment_subjectivity": sentiment.subjectivity
    }
    
    # Insert document into MongoDB collection
    collection.insert_one(document)

