from pyspark.sql import SparkSession
import threading
import time
import json
import requests
from kafka.errors import NoBrokersAvailable
import pandas as pd

# Kafka and Flask configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "air_travel"
FLASK_ENDPOINT = "http://flask_app:5000/ingest"

# Read airline data CSV into a Pandas DataFrame
csv_file = "/data/Raw_Airline_data.csv"
df = pd.read_csv(csv_file)

# Read countries data for latitude and longitude
countries_file = "/data/countries.csv"
countries_df = pd.read_csv(countries_file)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToFlask") \
    .getOrCreate()

# Load the countries.csv file into a Spark DataFrame
spark_countries_df = spark.read.csv(countries_file, header=True, inferSchema=True)

# Produce data to Kafka
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

    # Iterate through rows in the airline data CSV and send to Kafka
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


# Consume data from Kafka, enrich it, and send to Flask
def consume_and_send():
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    messages = kafka_df.selectExpr("CAST(value AS STRING) as message")

    # Parse the Kafka JSON data
    parsed_df = messages.selectExpr(
        "json_tuple(message, 'Passenger ID', 'First Name', 'Last Name', 'Airport Country Code', 'Flight Status') as (Passenger_ID, First_Name, Last_Name, Country_Code, Flight_Status)"
    )

    # Join with the countries DataFrame for latitude and longitude
    enriched_df = parsed_df.join(
        spark_countries_df,
        parsed_df["Country_Code"] == spark_countries_df["country"],
        "left"
    ).select(
        "Passenger_ID",
        "First_Name",
        "Last_Name",
        "Country_Code",
        "Flight_Status",
        "latitude",
        "longitude"
    )

    def send_to_flask(batch_df, batch_id):
        # Convert each batch to Pandas and send rows to Flask
        for row in batch_df.collect():
            data = {
                "Passenger_ID": row["Passenger_ID"],
                "First_Name": row["First_Name"],
                "Last_Name": row["Last_Name"],
                "Country_Code": row["Country_Code"],
                "Flight_Status": row["Flight_Status"],
                "Latitude": row["latitude"],
                "Longitude": row["longitude"]
            }
            response = requests.post(FLASK_ENDPOINT, json=data)
            print(f"Sent to Flask: {data} | Response: {response.status_code}")

    query = enriched_df.writeStream.foreachBatch(send_to_flask).start()
    query.awaitTermination()


# Run the Spark producer and consumer
if __name__ == "__main__":
    # Start producer in a separate thread
    producer_thread = threading.Thread(target=produce_data, daemon=True)
    producer_thread.start()

    # Start consuming, enriching, and sending data to Flask
    consume_and_send()
