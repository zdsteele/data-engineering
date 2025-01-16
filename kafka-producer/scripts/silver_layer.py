from kafka import KafkaConsumer, KafkaProducer
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

KAFKA_BROKER = "kafka:9092"
BRONZE_TOPIC = "bronze_topic"
SILVER_TOPIC = "silver_topic"


def initialize_consumer(topic, retries=5, delay=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            print(f"Connected to Kafka topic: {topic}")
            return consumer
        except Exception as e:
            print(
                f"Kafka consumer unavailable for {topic} (attempt {attempt + 1}/{retries}). Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception(f"Failed to connect to Kafka topic: {topic} after multiple attempts.")


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


def process_bronze_to_silver():
    consumer = initialize_consumer(BRONZE_TOPIC)
    producer = initialize_producer()

    # Initialize Spark for transformations
    spark = SparkSession.builder \
        .appName("SilverLayer") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    for message in consumer:
        bronze_data = message.value
        print(f"Received from Bronze: {bronze_data}")

        # Convert the received JSON data to a Spark DataFrame
        df_bronze = spark.read.json(spark.sparkContext.parallelize([bronze_data]))

        # Apply transformations (e.g., standardize date format)
        df_silver = df_bronze.withColumn("Departure Date", to_date("Departure Date", "MM/dd/yyyy"))

        # Collect transformed data and send to Silver Kafka topic
        silver_data = df_silver.toJSON().collect()
        for record in silver_data:
            producer.send(SILVER_TOPIC, value=json.loads(record))
            print(f"Sent to Silver: {json.loads(record)}")


if __name__ == "__main__":
    process_bronze_to_silver()
