from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def validate_data_quality(**kwargs):
        spark = SparkSession.builder \
            .appName("DataQualityChecks") \
            .getOrCreate()
        
        # Read silver data
        silver_df = spark.read.parquet("/home/leandro/Desktop/BEES Project/silver/breweries")
        
        # Check 1: Empty data
        if silver_df.count() == 0:
            raise ValueError("Silver layer contains no data!")
        
        # Check 2: Null values in critical columns
        critical_columns = ["id", "name", "brewery_type"]
        for column in critical_columns:
            null_count = silver_df.filter(col(column).isNull()).count()
            if null_count > 0:
                raise ValueError(f"Column {column} contains {null_count} null values")
        
        # Check 3: Valid brewery types
        valid_types = ["micro", "nano", "regional", "brewpub", "large", "planning"]
        invalid_types = silver_df.filter(
            ~col("brewery_type").isin(valid_types)
        )
        if invalid_types.count() > 0:
            raise ValueError(f"Found {invalid_types.count()} invalid brewery types")