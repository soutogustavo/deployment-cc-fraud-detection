# Simple consumer to read fraud alerts from Kafka
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from utils.config_loader import load_config


configuration = load_config()

KAFKA_SERVER = configuration["kafka"]["server"]
KAFKA_PORT = configuration["kafka"]["port"]
KAFKA_TOPIC_FRAUDS = configuration["kafka"]["topics"]["fraud_alerts"]

frauds_consumer = KafkaConsumer(
    bootstrap_servers=f"{KAFKA_SERVER}:{KAFKA_PORT}")
frauds_consumer.subscribe(topics=[KAFKA_TOPIC_FRAUDS])

for msg in frauds_consumer:
    print(msg)
