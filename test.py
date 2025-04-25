from pyspark.sql import SparkSession
from pyspark.sql.functions import count, filter

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ReadJSON") \
    .getOrCreate()

# Read the JSON file (replace with your actual file path)
df = spark.read.option("multiline","true").json("breweries.json")

# Show the data
df.show(truncate=False)