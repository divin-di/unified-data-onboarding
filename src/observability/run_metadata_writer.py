from pyspark.sql import Row

def write_run_metadata(spark, metadata, output_path):
    # Create a list of Spark Row objects
    # We loop through the list to ensure each item is a 'mapping'
    rows = [Row(**m) for m in metadata if m is not None]
    
    if not rows:
        print("No metadata to write.")
        return

    # Create a DataFrame from the list of Rows
    df = spark.createDataFrame(rows)
    
    # Write to disk
    df.write.mode("append").parquet(output_path)