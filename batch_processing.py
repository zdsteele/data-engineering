from pyspark.sql import SparkSession
import os

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
# os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")

# Initialize Spark session
spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()

# Read batch data
df = spark.read.csv("your_dataset.csv", header=True, inferSchema=True)

# Perform aggregation
result = df.groupBy("column_name").count()
result.show()
