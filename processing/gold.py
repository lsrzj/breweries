from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    import sys
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("Gold").getOrCreate()

    df = spark.read.parquet(input_path)

    # Business metrics
    (df.groupBy("brewery_type")
     .count()
     .write.mode("overwrite")
     .parquet(f"{output_path}/by_type"))

    (df.groupBy("country", "state")
     .count()
     .write.mode("overwrite")
     .parquet(f"{output_path}/by_location"))

    (df.groupBy("brewery_type", "country", "state")
     .count()
     .write.mode("overwrite")
        .parquet(f"{output_path}/by_type_location"))
