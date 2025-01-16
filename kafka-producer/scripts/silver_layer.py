from kafka import KafkaConsumer, KafkaProducer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StringType, IntegerType

KAFKA_BROKER = "kafka:9092"
BRONZE_TOPIC = "bronze_topic"
SILVER_TOPIC = "silver_topic"


def process_bronze_to_silver():
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        BRONZE_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Process data from Bronze to Silver
    for message in consumer:
        record = message.value

        # Example transformation: Format departure date
        if "Departure Date" in record:
            record["Departure Date"] = pd.to_datetime(record["Departure Date"], errors="coerce").strftime("%Y-%m-%d")

        # Stream cleaned data to Silver Topic
        producer.send(SILVER_TOPIC, value=record)
        print(f"Silver Layer: Sent {record}")


if __name__ == "__main__":
    process_bronze_to_silver()
