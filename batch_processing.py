from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()

# Read batch data
df = spark.read.csv("your_dataset.csv", header=True, inferSchema=True)

# Perform aggregation
result = df.groupBy("column_name").count()
result.show()
