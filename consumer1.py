from kafka import KafkaConsumer
import json
from itertools import combinations
import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["amazon_products"]
collection = db["frequent_itemsets"]

# Initialize Kafka consumer
consumer = KafkaConsumer('amazon_products',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Store itemsets and their counts
itemsets_count = {}

# Minimum support threshold
min_support = 2

def update_itemsets(items):
    for itemset in combinations(items, 2):  # Example with pairs; modify for more items
        if itemset in itemsets_count:
            itemsets_count[itemset] += 1
        else:
            itemsets_count[itemset] = 1

# Consuming messages
for message in consumer:
    product = message.value
    categories = product.get('categories', [])
    update_itemsets(categories)
    
    # Filter and insert frequent itemsets into MongoDB
    frequent_itemsets = {k: v for k, v in itemsets_count.items() if v >= min_support}
    if frequent_itemsets:
        for itemset, count in frequent_itemsets.items():
            document = {
                "itemset": list(itemset),
                "count": count
            }
            collection.insert_one(document)

