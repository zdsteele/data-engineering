from pyspark.sql import SparkSession

KAFKA_BROKER = "kafka:9092"
SILVER_TOPIC = "silver_topic"
GOLD_TOPIC = "gold_topic"

def pass_silver_to_gold():
    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()

    # Read from the Silver topic
    silver_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", SILVER_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Write to the Gold topic
    gold_query = silver_stream.selectExpr(
        "CAST(key AS STRING)", "CAST(value AS STRING)"
    ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", GOLD_TOPIC) \
        .option("checkpointLocation", "/app/checkpoints/silver_to_gold") \
        .start()

    gold_query.awaitTermination()

if __name__ == "__main__":
    pass_silver_to_gold()
