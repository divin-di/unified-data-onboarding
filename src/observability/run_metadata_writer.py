from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime

def write_run_metadata(spark, metadata_list, output_path: str):
    """
    Writes a list of metadata dictionaries to a Parquet audit table.
    """
    if not metadata_list:
        print("No metadata to write.")
        return

    # 1. Define the correct Schema
    # Ensure types match the actual data being passed
    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("dataset_name", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("status", StringType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_good", LongType(), True),
        StructField("records_bad", LongType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("environment", StringType(), True)
    ])

    rows = []
    for meta in metadata_list:
        # 2. Extract metrics (handling nested 'metrics' dict from our previous step)
        metrics = meta.get("metrics", {})
        
        # 3. Create Row object matching the Schema exactly
        row = Row(
            run_id=meta.get("run_id"),
            dataset_name=meta.get("dataset_name"),
            layer=meta.get("layer"),
            status=meta.get("status"),
            records_read=metrics.get("records_read", 0),
            records_good=metrics.get("records_good", 0),
            records_bad=metrics.get("records_bad", 0),
            start_time=meta.get("start_time"),
            end_time=meta.get("end_time"),
            environment=meta.get("environment")
        )
        rows.append(row)

    # 4. Create DataFrame and Write
    df = spark.createDataFrame(rows, schema=schema)
    
    # Write using append so we keep a history of all runs
    df.write.mode("append").parquet(output_path)
    print(f"Metadata successfully written to {output_path}")