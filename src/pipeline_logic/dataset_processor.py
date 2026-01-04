from datetime import datetime, timezone
from pyspark.sql import functions as F

def process_layer(spark, dataset_config, run_id, env, layer="bronze"):
    """
    Enterprise-standard generic processor for Bronze and Silver layers.
    """
    start_time = datetime.now(timezone.utc)
    name = dataset_config["name"]
    
    # 1. Configuration Selection
    # Safely get the config block for the specific layer
    cfg = dataset_config.get(f"{layer}_layer")
    if not cfg:
        raise ValueError(f"Configuration for {layer} layer not found for dataset {name}")

    try:
        # 2. READ PHASE
        if layer == "bronze":
            from readers.json_reader_spark import read_json
            df = read_json(spark, cfg["input"]["path"])
        else:
            df = spark.read.parquet(cfg["input"]["path"])
            # Add Silver-specific ingestion metadata
            df = df.withColumn("ingestion_date", F.current_date())
            df = df.withColumn("metadata_run_id", F.lit(run_id))

        records_read = df.count()

        # 3. TRANSFORMATION & VALIDATION PHASE
        if layer == "bronze":
            import json
            from validation.schema_validator_spark import validate_schema
            from validation.dq_validator import apply_dq_rules
            
            with open(dataset_config["schema"]["path"]) as f:
                expected_schema = json.load(f)

            schema_good_df, schema_bad_df = validate_schema(df, expected_schema)
            final_good_df, dq_bad_df = apply_dq_rules(schema_good_df)
            final_bad_df = schema_bad_df.unionByName(dq_bad_df, allowMissingColumns=True)
        
        elif layer == "silver":
            from transforms.bronze_to_silver import bronze_to_silver
            # Silver usually transforms 'good' data into business logic
            final_good_df = bronze_to_silver(df)
            final_bad_df = None # Silver failures are usually handled by exceptions

        # 4. WRITE PHASE
        if layer == "bronze":
            from writers.parquet_writer_spark import write_parquet
            write_parquet(final_good_df, cfg["output"]["good"])
            write_parquet(final_bad_df, cfg["output"]["bad"])
        else:
            from writers.silver_parquet_writer import parquet_writer_silver
            parquet_writer_silver(
                final_good_df, 
                cfg["output"]["path"], 
                mode="overwrite", 
                partition_cols="ingestion_date"
            )

        # 5. METADATA COLLECTION
        records_good = final_good_df.count()
        records_bad = final_bad_df.count() if final_bad_df else 0
        end_time = datetime.now(timezone.utc)
        duration_sec = (end_time - start_time).total_seconds()

        status = "FAILED" if records_good == 0 else "PARTIAL" if records_bad > 0 else "SUCCESS"

        metadata = {
            "run_id": run_id,
            "dataset_name": dataset_config["name"],
            "layer": layer,
            "status": status,
            "records_processed": records_read,
            "records_good": records_good,
            "records_bad": records_bad,
            "start_time": start_time,
            "end_time": end_time,
            "environment": env
            }
        
        
        # Write metadata to your observability store (Audit Table/S3)
        # write_run_metadata(metadata) 
        
        return metadata

    except Exception as e:
        # Enterprise error handling
        return {
            "run_id": run_id,
            "status": "CRITICAL_ERROR",
            "error_message": str(e),
            "layer": layer
        }