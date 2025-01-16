from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

KAFKA_BROKER = "kafka:9092"
SILVER_TOPIC = "silver_topic"


def process_silver_to_gold():
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        SILVER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    # Initialize Spark
    spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

    data = []
    for message in consumer:
        data.append(message.value)

        # Process batch of 100 records
        if len(data) >= 100:
            df = spark.createDataFrame(data)

            # Example aggregation: Top 10 countries by passenger count
            top_countries = df.groupBy("Country Name").agg(count("*").alias("Passenger Count"))
            top_countries = top_countries.orderBy(col("Passenger Count").desc()).limit(10)

            # Show results (or write to a database)
            top_countries.show()

            # Clear processed batch
            data = []


if __name__ == "__main__":
    process_silver_to_gold()
