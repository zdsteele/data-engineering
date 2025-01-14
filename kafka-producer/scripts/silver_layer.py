from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

# Initialize Spark
spark = SparkSession.builder \
    .appName("SilverLayer") \
    .getOrCreate()

# Read Bronze data
df_bronze = spark.read.csv("/app/data/bronze_stream", header=True, inferSchema=True)

# Transformations (cleaning and standardizing)
df_silver = df_bronze.withColumn("Departure Date", to_date("Departure Date", "MM/dd/yyyy"))

# Save to Silver layer
df_silver.write.mode("overwrite").csv("/app/data/silver_stream", header=True)
