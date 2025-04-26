from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path', required=True)
    parser.add_argument('--output-path', required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()
    
    try:
        df = spark.read.json(args.input_path)
        
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
          .parquet(args.output_path)
          
    except Exception as e:
        spark.stop()
        raise RuntimeError(f"Silver processing failed: {str(e)}")