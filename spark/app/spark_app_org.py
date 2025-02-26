from pyspark.sql import SparkSession
import threading
import time
import json
import requests
from kafka.errors import NoBrokersAvailable
import pandas as pd

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "air_travel"
FLASK_ENDPOINT = "http://flask_app:5000/ingest"


# Read data from the CSV file
csv_file = "/data/Raw_Airline_data.csv"
df = pd.read_csv(csv_file)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToFlask") \
    .getOrCreate()

# Produce fake data to Kafka
def produce_data():
    from kafka import KafkaProducer
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            break
        except NoBrokersAvailable:
            print("Kafka broker not available. Retrying in 5 seconds...")
            time.sleep(5)

    # Iterate through rows in the CSV and send to Kafka
    for _, row in df.iterrows():
        # Convert row to a dictionary
        data = row.to_dict()

        # Send row data as a message to Kafka
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Produced: {data}")
        time.sleep(1)  # Adjust the delay if needed

    # Flush and close the producer
    producer.flush()
    producer.close()


# Consume data from Kafka and send to Flask
def consume_and_send():
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    messages = kafka_df.selectExpr("CAST(value AS STRING) as message")

    def send_to_flask(batch_df, batch_id):
        for row in batch_df.collect():
            response = requests.post(FLASK_ENDPOINT, json={"message": row["message"]})
            print(f"Sent to Flask: {row['message']} | Response: {response.status_code}")

    query = messages.writeStream.foreachBatch(send_to_flask).start()
    query.awaitTermination()

# Run the Spark producer and consumer
if __name__ == "__main__":
    # Start producer in a separate thread
    producer_thread = threading.Thread(target=produce_data, daemon=True)
    producer_thread.start()

    # Start consuming and sending data to Flask
    consume_and_send()
