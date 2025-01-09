import os, datetime
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, col, sum, lit
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SampleDataFrame") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Sample data
data = [
    (1, "Alice", 29, 72000.00),
    (2, "Bob", 35, 85000.00),
    (3, "Charlie", 25, 48000.00),
    (4, "Diana", 40, 96000.00)
]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()

# Perform a simple transformation
df_with_bonus = df.withColumn("bonus", df.salary * 0.10)

# Show the transformed DataFrame
df_with_bonus.show()

# Stop Spark Session
spark.stop()