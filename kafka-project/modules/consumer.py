from json import loads

from kafka import KafkaConsumer
from pymongo import MongoClient

bootstrap_servers = ['localhost:9092']
topicName = 'topic1'
key1 = "api"
key2 = "csv"

client = MongoClient('localhost', 27017)
db = client.financedb

api_data_collection = db.api_data_collection
csv_data_collection = db.csv_data_collection


def start_consumer():
    consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest', key_deserializer=str.decode(),
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
    for msg in consumer:
        if msg.key == key1:
            api_data_collection.insert_one(msg.value)
        elif msg.key == key2:
            csv_data_collection.insert_one(msg.value)
