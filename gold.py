from pyspark.sql.functions import count
from pyspark.sql import SparkSession

def create_gold_views(**kwargs):
        spark = SparkSession.builder \
            .appName("BreweryGold") \
            .getOrCreate()
        
        # Read silver data
        silver_df = spark.read.parquet("/home/leandro/Desktop/BEES Project/silver/breweries")
        
        # Breweries by type and location
        by_type_location = silver_df.groupBy(
            "brewery_type", "country", "state"
        ).agg(count("*").alias("brewery_count"))
        
        by_type_location.write.mode("overwrite") \
            .parquet("/home/leandro/Desktop/BEES Project/gold/breweries_by_type_location")
        
        # Breweries by type
        by_type = silver_df.groupBy("brewery_type") \
            .agg(count("*").alias("brewery_count"))
        
        by_type.write.mode("overwrite") \
            .parquet("/home/leandro/Desktop/BEES Project/gold/breweries_by_type")
        
        # Breweries by location
        by_location = silver_df.groupBy("country", "state") \
            .agg(count("*").alias("brewery_count"))
        
        by_location.write.mode("overwrite") \
            .parquet("/home/leandro/Desktop/BEES Project/gold/breweries_by_location")