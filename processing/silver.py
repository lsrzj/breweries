from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

if __name__ == "__main__":
        
    spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()
    
    try:
        df = spark.read.json(sys.argv[1])
        
        # Clean data
        df = (df
            .withColumn("brewery_type", lower(col("brewery_type")))
            .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
            .withColumn("state", coalesce(col("state"), col("state_province")))
            .withColumn("street", coalesce(col("street"), col("address_1")))
            .drop("address_1", "state_province")
        )
        
        # Write partitioned
        df.write.mode("overwrite") \
          .partitionBy("country", "state") \
          .parquet(sys.argv[2])
          
    except Exception as e:
        spark.stop()
        raise RuntimeError(f"Silver processing failed: {str(e)}")