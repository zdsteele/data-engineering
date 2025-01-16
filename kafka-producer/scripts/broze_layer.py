from kafka import KafkaProducer
import pandas as pd
import json
import time

KAFKA_BROKER = "kafka:9092"
BRONZE_TOPIC = "bronze_topic"

def stream_to_bronze(csv_path):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    data = pd.read_csv(csv_path)
    for _, row in data.iterrows():
        producer.send(BRONZE_TOPIC, value=row.to_dict())
        print(f"Bronze Layer: Sent {row.to_dict()}")
        time.sleep(1)  # Simulate streaming delay

if __name__ == "__main__":
    csv_path = "/app/data/Raw_Airline_data.csv"
    stream_to_bronze(csv_path)
