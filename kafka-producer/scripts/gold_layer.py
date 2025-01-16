from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time
import json

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
SILVER_TOPIC = "silver_topic"
GOLD_TOPIC = "gold_topic"

def ensure_topic_exists(topic, retries=5, delay=5):
    """
    Ensure the specified Kafka topic exists, create if missing.
    """
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    for attempt in range(retries):
        available_topics = admin_client.list_topics()
        if topic in available_topics:
            print(f"Topic '{topic}' is available.")
            return
        try:
            print(f"Creating topic: {topic}")
            admin_client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            return
        except Exception as e:
            print(f"Failed to create topic '{topic}' (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception(f"Topic '{topic}' could not be created after {retries} retries.")

def initialize_broker(retries=5, delay=5):
    """
    Wait for Kafka broker to become available.
    """
    for attempt in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            print("Kafka broker is available.")
            producer.close()
            return
        except Exception as e:
            print(f"Kafka broker unavailable (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception("Kafka broker unavailable after multiple retries.")

def process_silver_to_gold():
    """
    Process data from the silver Kafka topic and write aggregated results to the gold Kafka topic.
    """
    # Initialize Spark session with Kafka dependencies
    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
        .getOrCreate()

    # Define JSON schema
    json_schema = StructType([StructField("Country Name", StringType(), True)])

    # Read data from the Silver Kafka topic as a streaming source
    silver_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", SILVER_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Debug input schema
    print("Silver stream schema:")
    silver_stream.printSchema()

    # Extract JSON payload
    silver_stream = silver_stream.selectExpr("CAST(value AS STRING) as json_data")

    # Debug the raw JSON data
    print("Raw JSON data:")
    silver_stream.writeStream \
        .format("console") \
        .start()

    # Parse JSON fields
    silver_stream_parsed = silver_stream.select(
        from_json(col("json_data"), json_schema).alias("data")
    ).select("data.*")

    # Debug parsed schema
    print("Parsed stream schema:")
    silver_stream_parsed.printSchema()

    # Add timestamp and watermark
    silver_stream_with_watermark = silver_stream_parsed.withColumn(
        "timestamp", current_timestamp()
    ).withWatermark("timestamp", "10 minutes")

    # Debug after adding timestamp
    print("Stream with watermark schema:")
    silver_stream_with_watermark.printSchema()

    # Aggregate data: Count passengers by country
    top_countries = silver_stream_with_watermark.groupBy("Country Name") \
        .agg(count("*").alias("Passenger Count"))

    # Debug aggregation result
    print("Aggregation schema:")
    top_countries.printSchema()

    # Write aggregated results to the Gold Kafka topic
    query = top_countries.selectExpr("to_json(struct(*)) AS value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", GOLD_TOPIC) \
        .option("checkpointLocation", "/app/checkpoints/gold_layer") \
        .outputMode("complete") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    # Ensure Kafka broker is available
    initialize_broker()

    # Ensure required Kafka topic exists
    ensure_topic_exists(GOLD_TOPIC)

    # Start processing data from silver to gold
    process_silver_to_gold()
