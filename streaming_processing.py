from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# Kafka topic schema
schema = "column_name STRING, value STRING"

# Initialize Spark session
spark = SparkSession.builder.appName("StreamingProcessing").getOrCreate()

# Read data from Kafka
streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "your_topic") \
    .load()

# Parse JSON data
parsed_df = streaming_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Perform streaming aggregation
result = parsed_df.groupBy("column_name").count()

# Output to console
query = result.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
