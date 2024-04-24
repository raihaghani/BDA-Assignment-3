from kafka import KafkaConsumer
import json
from itertools import combinations
import hashlib
import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["amazon_products"]
collection = db["frequent_itemsets_pcy"]

# Initialize Kafka consumer
consumer = KafkaConsumer('amazon_products',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Store itemsets and hash bucket counts
itemsets_count = {}
buckets = {}

# Minimum support and bucket size
min_support = 2
bucket_size = 10000

def hash_itemset(itemset):
    return int(hashlib.md5(json.dumps(sorted(itemset)).encode()).hexdigest(), 16) % bucket_size

def update_itemsets_and_buckets(items):
    for itemset in combinations(items, 2):  # Adjust the range as necessary
        hashed = hash_itemset(itemset)
        buckets[hashed] = buckets.get(hashed, 0) + 1

        if itemset in itemsets_count:
            itemsets_count[itemset] += 1
        else:
            itemsets_count[itemset] = 1

for message in consumer:
    product = message.value
    categories = product.get('categories', [])
    update_itemsets_and_buckets(categories)
    
    # Apply PCY conditions to filter frequent itemsets and insert into MongoDB
    frequent_itemsets = {k: v for k, v in itemsets_count.items() if v >= min_support and buckets[hash_itemset(k)] >= min_support}
    if frequent_itemsets:
        for itemset, count in frequent_itemsets.items():
            document = {
                "itemset": list(itemset),
                "count": count
            }
            collection.insert_one(document)

