from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import time
import uuid

KAFKA_BROKER = "kafka:9092"
BRONZE_TOPIC = "bronze_topic"
SILVER_TOPIC = "silver_topic"

def generate_checkpoint_path(base_path="/app/checkpoints/bronze_to_silver"):
    unique_id = str(uuid.uuid4())
    return f"{base_path}/{unique_id}"

def initialize_consumer(broker, topic, retries=5, delay=5):
    print(f"Attempting to initialize Kafka consumer for topic {topic}...")
    for attempt in range(retries):
        try:
            print(f"Attempt {attempt + 1}: Connecting to Kafka broker {broker}...")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=broker,
                auto_offset_reset="earliest"
            )
            print("Kafka Consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"Kafka Consumer unavailable (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception(f"Failed to connect to Kafka Consumer after {retries} retries.")


def wait_for_topic_with_data(broker, topic, timeout=30, retries=5, delay=5):
    """
    Wait for a topic to have data before starting the Spark job.
    """
    print(f"Checking for data in topic {topic}")
    consumer = initialize_consumer(broker, topic, retries, delay)
    start_time = time.time()

    while time.time() - start_time < timeout:
        for message in consumer:
            print(f"Data detected in topic {topic}: {message.value}")
            consumer.close()
            return True
        time.sleep(1)

    consumer.close()
    raise Exception(f"Timeout: No data in topic {topic} after {timeout} seconds")

def process_bronze_to_silver():
    """
    Read data from the Bronze Kafka topic and write to the Silver Kafka topic using Spark Structured Streaming.
    """
    print("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("SilverLayer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.kafka:kafka-clients:3.4.1") \
        .getOrCreate()
    print("Spark session initialized.")

    # Read from the Bronze topic
    bronze_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", BRONZE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Started reading from Bronze topic")

    # Write to the Silver topic
    silver_query = bronze_stream.selectExpr(
        "CAST(key AS STRING)", "CAST(value AS STRING)"
    ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", SILVER_TOPIC) \
        .option("checkpointLocation", generate_checkpoint_path()) \
        .start()

    print("Started writing to Silver topic")
    silver_query.awaitTermination()

def process_bronze_to_silver_with_retry(retries=5, delay=10):
    """
    Process Bronze to Silver with retry logic to handle transient failures.
    """
    for attempt in range(retries):
        try:
            process_bronze_to_silver()
            return
        except Exception as e:
            print(f"Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)
    raise Exception(f"Failed to process Bronze to Silver after {retries} retries.")

if __name__ == "__main__":
    print("Starting Silver Layer script...")
    print("Checking for environment variables...")
    print(f"KAFKA_BROKER: {KAFKA_BROKER}")
    print(f"BRONZE_TOPIC: {BRONZE_TOPIC}")
    print(f"SILVER_TOPIC: {SILVER_TOPIC}")

    print("Checking for data in Bronze topic...")
    wait_for_topic_with_data(KAFKA_BROKER, BRONZE_TOPIC)
    print("Data confirmed in Bronze topic. Proceeding to process Bronze to Silver.")

    print("Starting Bronze to Silver processing...")
    process_bronze_to_silver_with_retry()
    print("Processing completed.")

