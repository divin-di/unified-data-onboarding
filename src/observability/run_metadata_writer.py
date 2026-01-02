from pyspark.sql import Row

def write_run_metadata(spark, metadata, output_path):
    row = Row(**metadata)
    df = spark.createDataFrame([row])
    df.write.mode("append").parquet(output_path)
