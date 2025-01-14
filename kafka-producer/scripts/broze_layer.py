from pyspark.sql import SparkSession

# Kafka and Spark Configuration
KAFKA_TOPIC = "airline_data"
KAFKA_BROKER = "kafka:9092"

# Initialize Spark
spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .getOrCreate()

# Read Kafka Stream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Save raw data to Bronze layer
query = raw_stream.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/app/data/bronze") \
    .option("checkpointLocation", "/app/data/checkpoints/bronze") \
    .start()

query.awaitTermination()
