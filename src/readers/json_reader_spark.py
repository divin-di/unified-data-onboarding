from pyspark.sql import SparkSession

def read_json(spark: SparkSession, path: str):
    return spark.read.option("multiline", "true").json(path)
