import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

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
        df = spark.read.schema(schema).json(sys.argv[1]).cache()
        # Clean data
        df = (df
              # standardize column case
              .withColumn("brewery_type", lower(col("brewery_type")))
              # Country and state will be used for partitioning, so we need to lowercase them to avoid creating multiple partitions for the same country/state
              .withColumn("country", lower(col("country")))
              .withColumn("state", lower(col("state")))
              # remove any non-numeric characters from phone
              .withColumn("phone", regexp_replace(col("phone"), r"[^\p{N}]", ""))
              # removing duplicate meaning columns and unifying into one as state and state_province are the same when state_province is not null
              .withColumn("state", coalesce(col("state"), col("state_province")))
              # removing duplicate meaning columns and unifying into one as street and address_1 are the same when state_province is not null
              .withColumn("address_1", coalesce(col("street"), col("address_1")))
              # removing any non letter and non space characters from country
              .withColumn("country", regexp_replace(col("country"), r"[^\p{L}\p{Z}]", ""))
              # removing any non letter, non space, non dash and non initial punctuation characters from state
              .withColumn("state", regexp_replace(col("state"), r"[^\p{L}\p{Z}\p{Pd}\p{Pi}]", ""))
              # removing any non letter, non space, non dash and non initial punctuation characters from city
              .withColumn("city", regexp_replace(col("city"), r"[^\p{L}\p{Z}\p{Pd}\p{Pi}]", ""))
              # removing any non letter, non space, non dash, non punctuation characters, characters different from [&'-,] from address_1, address_2 and address_3
              .withColumn("address_1", regexp_replace(col("address_1"), r"[^\p{L}\p{Z}\p{Pd}\p{Pi}\p{N}&-',]", ""))
              .withColumn("address_2", regexp_replace(col("address_2"), r"[^\p{L}\p{Z}\p{Pd}\p{Pi}\p{N}&-',]", ""))
              .withColumn("address_3", regexp_replace(col("address_3"), r"[^\p{L}\p{Z}\p{Pd}\p{Pi}\p{N}&-',]", ""))
              # transforming all two or more blanks into one space for address_1, address_2, address_3
              .withColumn("address_1", regexp_replace(col("address_1"), r"\p{Z}+", " "))
              .withColumn("address_2", regexp_replace(col("address_2"), r"\p{Z}+", " "))
              .withColumn("address_3", regexp_replace(col("address_3"), r"\p{Z}+", " "))
              # trimming city, address_1, address_2 and address_3 to remove any leading or trailing spaces
              .withColumn("city", trim(col("city")))
              .withColumn("address_1", trim(col("address_1")))
              .withColumn("address_2", trim(col("address_2")))
              .withColumn("address_3", trim(col("address_3")))
              # trimming city, street, country and state to remove any leading or trailing spaces to avoid creating multiple partitions for the same country/state
              .withColumn("country", trim(col("country")))
              .withColumn("state", trim(col("state")))
              # dropping columns that are not needed, they were unified into one
              .drop("street", "state_province")
              )
        # Calculating the number of output partitions to try to achieve the target output partition size to avoid small files
        # it was generating 3.5Kb files, so we need to calculate the number of partitions to achieve 128MB files
        targetOutputPartitionSizeMB = 128
        parquetCompressionRatio = 0.1
        numOutputPartitions = 0.0035 * parquetCompressionRatio / targetOutputPartitionSizeMB

        # Write partitioned
        df.coalesce(math.ceil(numOutputPartitions)) \
            .write.mode("overwrite") \
          .partitionBy("country", "state") \
          .parquet(sys.argv[2])

    except Exception as e:
        spark.stop()
        raise RuntimeError(f"Silver processing failed: {str(e)}")
