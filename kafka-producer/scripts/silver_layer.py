from pyspark.sql import SparkSession

KAFKA_BROKER = "kafka:9092"
BRONZE_TOPIC = "bronze_topic"
SILVER_TOPIC = "silver_topic"

def process_bronze_to_silver():
    spark = SparkSession.builder \
        .appName("SilverLayer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.kafka:kafka-clients:3.4.1") \
        .getOrCreate()

    # Read from the Bronze topic
    bronze_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", BRONZE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Write to the Silver topic
    silver_query = bronze_stream.selectExpr(
        "CAST(key AS STRING)", "CAST(value AS STRING)"
    ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", SILVER_TOPIC) \
        .option("checkpointLocation", "/app/checkpoints/bronze_to_silver") \
        .start()

    silver_query.awaitTermination()

if __name__ == "__main__":
    process_bronze_to_silver()
