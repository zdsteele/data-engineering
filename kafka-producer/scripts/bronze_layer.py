from kafka import KafkaProducer
import pandas as pd
import json
import time

KAFKA_BROKER = "kafka:9092"
BRONZE_TOPIC = "bronze_topic"

def initialize_producer(retries=5, delay=5):
    """
    Initialize a Kafka producer with retry logic.
    Retries connecting to Kafka broker up to `retries` times with a `delay` between attempts.
    """
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka broker.")
            return producer
        except Exception as e:
            print(f"Kafka broker unavailable (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception(f"Failed to connect to Kafka broker after {retries} retries.")


def stream_to_bronze(csv_path):
    producer = initialize_producer()
    data = pd.read_csv(csv_path)
    for _, row in data.iterrows():
        producer.send(BRONZE_TOPIC, value=row.to_dict())
        print(f"Bronze Layer: Sent {row.to_dict()}")
        time.sleep(1)  # Simulate streaming delay

if __name__ == "__main__":
    csv_path = "/app/data/Raw_Airline_data.csv"
    stream_to_bronze(csv_path)
