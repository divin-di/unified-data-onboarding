from pyspark.sql import SparkSession
from transforms.bronze_to_silver import bronze_to_silver
from writers.silver_parquet_writer import parquet_writer_silver
from utils.config_loader import load_config
from pyspark.sql.functions import current_date
import os
import sys

# This prevents Spark from trying to use Unix Domain Sockets on Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

config = load_config("configs/pipeline.yml")

silver_cfg = config["datasets"][0]["silver_layer"]

spark = SparkSession.builder \
    .appName(silver_cfg["spark_app_name"]) \
    .getOrCreate()

bronze_good_path = silver_cfg["input"]["path"]
silver_path = silver_cfg["output"]["path"]
write_mode = "overwrite"

bronze_df = spark.read.parquet(bronze_good_path)
bronze_df = bronze_df.withColumn("ingestion_date", current_date())

silver_df = bronze_to_silver(bronze_df)

parquet_writer_silver(
    silver_df,
    silver_path,
    mode=write_mode,
    partition_cols="ingestion_date"  # important for idempotency
)

spark.stop()
