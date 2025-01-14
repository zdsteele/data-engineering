from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import threading
import json
import time


app = Flask(__name__)

KAFKA_TOPIC = "airline_data"
KAFKA_BROKER = "kafka:9092"

# Shared data for real-time updates
data_rows = []


def initialize_kafka_consumer():
    """Initialize Kafka consumer with retries."""
    retries = 5
    delay = 5
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            print("Kafka Consumer connected.")
            return consumer
        except Exception as e:
            print(f"Kafka broker unavailable (attempt {attempt + 1}/{retries}). Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka broker after multiple attempts.")


def consume_kafka_data():
    """Consume Kafka messages and update the shared data structure."""
    global data_rows
    consumer = initialize_kafka_consumer()
    for message in consumer:
        data_rows.append(message.value)
        if len(data_rows) > 100:  # Limit data size for display
            data_rows.pop(0)


# Start Kafka consumer in a separate thread
threading.Thread(target=consume_kafka_data, daemon=True).start()

@app.route('/')
def index():
    return render_template("index.html", data_rows=data_rows)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
