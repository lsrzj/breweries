from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

if __name__ == "__main__":
    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("brewery_type", StringType()),
        StructField("address_1", StringType()),
        StructField("address_2", StringType()),
        StructField("address_3", StringType()),
        StructField("city", StringType()),
        StructField("state_province", StringType()),
        StructField("postal_code", StringType()),
        StructField("country", StringType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("phone", StringType()),
        StructField("website_url", StringType()),
        StructField("state", StringType()),
        StructField("street", StringType())
    ])
    spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()

    try:
        df = spark.read.schema(schema).json(sys.argv[1])
        df.where(col("state_province") != col("state") & col("state_province").isNotNull()).select(col("state"), col("state_province")).show(df.count(), truncate=False)
        exit(1)
        # Clean data
        df = (df
              # standardize column case
              .withColumn("brewery_type", lower(col("brewery_type")))
              # remove any non-numeric characters from phone
              .withColumn("phone", regexp_replace(col("phone"), r"[^\p{N}]", ""))
              # 
              .withColumn("state", coalesce(col("state"), col("state_province")))
              .withColumn("street", coalesce(col("street"), col("address_1")))
              .withColumn("country", regexp_replace(col("country"), r"[^\p{L}\p{Z}]", ""))
              .withColumn("state", regexp_replace(col("state"), r"[^\p{L}\p{Z}\p{Pd}\p{Pi}]", ""))
              .withColumn("street", regexp_replace(col("street"), r"[^\p{L}\p{Z}\p{Pd}\p{Pi}\p{N}&']", ""))
              .withColumn("street", regexp_replace(col("street"), r"\p{Z}+", " "))
              .withColumn("city", trim(col("state")))
              .withColumn("street", trim(col("street")))
              .withColumn("country", trim(col("country")))
              .withColumn("state", trim(col("state")))
              .withcolumn("address", concat_ws(", ", col("address_1"), col("address_2"), col("address_3")))
              .drop("address_1", "address_2", "address_3", "state_province")
              )

        # Write partitioned
        df.write.mode("overwrite") \
          .partitionBy("country", "state") \
          .parquet(sys.argv[2])

    except Exception as e:
        spark.stop()
        raise RuntimeError(f"Silver processing failed: {str(e)}")
