import time
import pandas as pd
import json
from kafka import KafkaProducer

KAFKA_TOPIC = "airline_data"
KAFKA_BROKER = "kafka:9092"

def initialize_producer(retries=5, delay=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka broker.")
            return producer
        except Exception as e:
            print(f"Kafka broker unavailable (attempt {attempt + 1}/{retries}). Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka broker after multiple attempts.")


def stream_csv_to_kafka(csv_path):
    producer = initialize_producer()
    data = pd.read_csv(csv_path)
    for _, row in data.iterrows():
        producer.send(KAFKA_TOPIC, value=row.to_dict())
        print(f"Sent: {row.to_dict()}")
        time.sleep(1)  # Simulate streaming delay


if __name__ == "__main__":
    csv_path = "/app/data/Raw_Airline_data.csv"
    stream_csv_to_kafka(csv_path)
