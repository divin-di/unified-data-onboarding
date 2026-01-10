from datetime import datetime, timezone
import sys
import os
from pyspark.sql import functions as F


# Get the root of your git repo dynamically
repo_root = "/Workspace/Users/divinmr@gmail.com/unified-data-onboarding"
if repo_root not in sys.path:
    sys.path.append(repo_root)

#  Import everything at the top (standard Python practice)
try:
    from src.readers.json_reader_spark import read_json
    from src.readers.universal_reader import read_input

    from src.validation.schema_validator_spark import validate_schema
    from src.validation.dq_validator import apply_dq_rules
    from src.transforms.bronze_to_silver import bronze_to_silver
    from src.writers.parquet_writer_spark import write_parquet
    from src.writers.silver_parquet_writer import parquet_writer_silver
except ImportError as e:
    print(f"Check your folder structure! Import failed: {e}")

def process_layer(spark, dataset_config, run_id, env, layer="bronze"):
    start_time = datetime.now(timezone.utc)
    name = dataset_config["name"]
    
    
    cfg = dataset_config.get(f"{layer}_layer")
    if not cfg:
        raise ValueError(f"Configuration for {layer} layer not found for dataset {name}")

    try:
        # 1. READ PHASE
        # These paths should be your /mnt/... locations
        input_path = cfg["input"]["path"]
        
        if layer == "bronze":
            df = read_input(spark, dataset_config)
        else:
            df = spark.read.parquet(input_path)
            df = df.withColumn("ingestion_date", F.current_date()) \
                   .withColumn("metadata_run_id", F.lit(run_id))

        records_read = df.count()

        # 2. TRANSFORMATION & VALIDATION
        if layer == "bronze":
            import json
            # Ensure schema path is an absolute Workspace path
            with open(dataset_config["schema"]["path"]) as f:
                expected_schema = json.load(f)

            schema_good_df, schema_bad_df = validate_schema(df, expected_schema)
            final_good_df, dq_bad_df = apply_dq_rules(schema_good_df)
            
            # Combine all bad data (Schema failures + DQ failures)
            final_bad_df = schema_bad_df.unionByName(dq_bad_df, allowMissingColumns=True)
        
        elif layer == "silver":
            final_good_df = bronze_to_silver(df)
            final_bad_df = None 

        # 3. WRITE PHASE
        if layer == "bronze":
            # Dumps "Good" data to one subfolder and "Bad" to the error subfolder
            write_parquet(final_good_df, cfg["output"]["good"])
            write_parquet(final_bad_df, cfg["output"]["bad"])
        else:
            parquet_writer_silver(
                final_good_df, 
                cfg["output"]["path"], 
                mode="overwrite", 
                partition_cols=["ingestion_date"] # Note: list format is safer
            )

        # 4. METADATA COLLECTION
        records_good = final_good_df.count()
        records_bad = final_bad_df.count() if final_bad_df else 0
        
        status = "SUCCESS"
        if records_good == 0: status = "FAILED"
        elif records_bad > 0: status = "PARTIAL"

        return {
            "run_id": run_id,
            "dataset_name": name,
            "layer": layer,
            "status": status,
            "records_processed": records_read,
            "records_good": records_good,
            "records_bad": records_bad,
            "duration_sec": (datetime.now(timezone.utc) - start_time).total_seconds(),
            "environment": env
        }

    except Exception as e:
        return {
            "run_id": run_id,
            "status": "CRITICAL_ERROR",
            "error_message": str(e),
            "layer": layer
        }