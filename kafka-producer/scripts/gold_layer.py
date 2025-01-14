from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark
spark = SparkSession.builder \
    .appName("GoldLayer") \
    .getOrCreate()

# Read Silver data
df_silver = spark.read.csv("/app/data/silver_stream", header=True, inferSchema=True)

# Example aggregation: Top 10 countries by passenger count
top_countries = df_silver.groupBy("Country Name").agg(count("*").alias("Passenger Count"))
top_countries = top_countries.orderBy(col("Passenger Count").desc()).limit(10)

# Save to Gold layer
top_countries.write.mode("overwrite").csv("/app/data/gold_stream", header=True)
