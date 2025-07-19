import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import csv
import json
from kafka import KafkaProducer
from utils.config_loader import load_config


configuration = load_config()
KAFKA_SERVER = configuration["kafka"]["server"]
KAFKA_PORT = configuration["kafka"]["port"]
KAFKA_TOPIC_TRANSACTIONS = configuration["kafka"]["topics"]["transactions"]


producer = KafkaProducer(
    bootstrap_servers=f"{KAFKA_SERVER}:{KAFKA_PORT}",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    retries=3,
)

with open("data/stream_data_test.csv", "r") as dta:
    reader = csv.DictReader(dta)
    for row in reader:
        key = row.get("transaction_id")
        producer.send(KAFKA_TOPIC_TRANSACTIONS, key=key, value=row)
        time.sleep(0.1)  # Simulate a delay between messages
        
producer.flush()
producer.close()
