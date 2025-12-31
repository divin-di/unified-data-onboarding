from pyspark.sql import SparkSession
from transforms.bronze_to_silver import bronze_to_silver
from writers.parquet_writer_spark import write_parquet
from utils.config_loader import load_config
import os
import sys

# This prevents Spark from trying to use Unix Domain Sockets on Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

config = load_config("configs/pipeline.yml")

silver_cfg = config["silver_layer"]

spark = SparkSession.builder \
    .appName(silver_cfg["spark_app_name"]) \
    .getOrCreate()

bronze_good_path = silver_cfg["input"]["path"]
silver_path = silver_cfg["output"]["path"]
write_mode = silver_cfg["output"]["mode"]

bronze_df = spark.read.parquet(bronze_good_path)
silver_df = bronze_to_silver(bronze_df)

write_parquet(
    silver_df,
    silver_path,
    mode=write_mode  # important for idempotency
)

spark.stop()
