from pyspark.sql import SparkSession
from transforms.bronze_to_silver import bronze_to_silver
from writers.parquet_writer_spark import write_parquet
import os
import sys

# This prevents Spark from trying to use Unix Domain Sockets on Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("SilverPipeline") \
    .getOrCreate()

bronze_good_path = "data/bronze/good"
silver_path = "data/silver/transactions"

bronze_df = spark.read.parquet(bronze_good_path)
silver_df = bronze_to_silver(bronze_df)

write_parquet(
    silver_df,
    silver_path,
    mode="overwrite"  # important for idempotency
)

spark.stop()
