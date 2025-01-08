from pyspark.sql import SparkSession
import os


print('Hello World')
# Check and print JAVA_HOME
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("BatchProcessing")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "1g")
    .getOrCreate()
)

# Read batch data (ensure the path is correct)
csv_path = "data/data.csv"  # Adjust this path based on your folder structure
print(f"Reading data from {csv_path}...")
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Perform aggregation
print("Performing aggregation...")
result = df.groupBy("column_name").count()

# Show the result
print("Aggregation result:")
result.show()

# Stop the Spark session
spark.stop()

