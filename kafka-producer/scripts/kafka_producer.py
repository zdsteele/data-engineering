import os
import time
import subprocess
import pandas as pd
import json
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_TOPIC = "airline_data"
KAFKA_BROKER = "kafka:9092"

def initialize_kafka_producer(broker, retries=5, delay=5):
    """Initialize Kafka producer with retries."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Kafka Producer connected.")
            return producer
        except Exception as e:
            print(f"Kafka not available (attempt {attempt + 1}/{retries}). Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Could not connect to Kafka after multiple attempts.")

def stream_data_to_kafka(producer):
    """Streams data from Raw_Airline_data.csv to Kafka topic."""
    print("Starting Kafka Producer...")
    try:
        # Load data
        data_path = "/app/data/Raw_Airline_data.csv"
        data = pd.read_csv(data_path)

        # Stream data row by row
        for _, row in data.iterrows():
            producer.send(KAFKA_TOPIC, value=row.to_dict())
            print(f"Sent: {row.to_dict()}")
            time.sleep(1)  # Simulate real-time streaming

        print("Data streaming completed.")
    except Exception as e:
        print(f"Error while streaming data: {e}")

def run_script(script_path):
    """Run a Python script."""
    try:
        print(f"Running {script_path}...")
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error while running {script_path}: {e}")

if __name__ == "__main__":
    # Step 1: Ensure required directories exist
    os.makedirs("/app/data/bronze_stream", exist_ok=True)
    os.makedirs("/app/data/silver_stream", exist_ok=True)
    os.makedirs("/app/data/gold_stream", exist_ok=True)

    # Step 2: Stream data to Kafka
    producer = initialize_kafka_producer(KAFKA_BROKER)
    stream_data_to_kafka(producer)

    # Step 3: Run Bronze Layer
    run_script("/app/scripts/bronze_layer.py")

    # Step 4: Run Silver Layer
    run_script("/app/scripts/silver_layer.py")

    # Step 5: Run Gold Layer
    run_script("/app/scripts/gold_layer.py")

    print("Pipeline completed.")
