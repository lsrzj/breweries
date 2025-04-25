from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, lower, regexp_replace, lit


def process_silver_data(**kwargs):
    spark = SparkSession.builder \
        .appName("BrewerySilver") \
        .getOrCreate()
    
    # Read bronze data
    bronze_df = spark.read.json("/home/leandro/Desktop/BEES Project/bronze/breweries_raw")
    
    # Transformations
    silver_df = bronze_df.withColumn(
        "brewery_type", lower(col("brewery_type"))
    ).withColumn(
        "phone", regexp_replace(col("phone"), "[^0-9]", "")
    ).withColumn(
        "latitude", col("latitude").cast(FloatType())
    ).withColumn(
        "longitude", col("longitude").cast(FloatType())
    ).withColumn(
        "ingestion_timestamp", lit(datetime.now().isoformat())
    ).fillna({
        "state": "unknown",
        "country": "unknown"
    })
    
    # Write as partitioned Parquet
    silver_df.write.mode("overwrite") \
        .partitionBy("country", "state") \
        .parquet("/home/leandro/Desktop/BEES Project/silver/breweries")
    
    # Create Hive table (optional)
    silver_df.write.mode("overwrite") \
        .partitionBy("country", "state") \
        .saveAsTable("silver_breweries")

