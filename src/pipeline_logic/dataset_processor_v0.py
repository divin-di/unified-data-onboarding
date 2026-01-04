
def process_dataset(spark, dataset_config, run_id, env):
    import json
    from datetime import datetime, timezone
    import sys
    import yaml
    from pyspark.sql import SparkSession
    from readers.json_reader_spark import read_json
    from validation.schema_validator_spark import validate_schema
    from validation.dq_validator import apply_dq_rules
    from writers.parquet_writer_spark import write_parquet
    from utils.config_loader import load_config
    from observability.run_metadata_writer import write_run_metadata

    start_time = datetime.now(timezone.utc)
    
    name = dataset_config["name"]
    input_path = dataset_config["bronze_layer"]["input"]["path"]
    schema_path = dataset_config["schema"]["path"]
    bronze_good = dataset_config["bronze_layer"]["output"]["good"]
    bronze_bad = dataset_config["bronze_layer"]["output"]["bad"]

    # Read data
    df = read_json(spark,input_path )
    records_read = df.count()

    # Load schema
    with open(schema_path) as f:
        expected_schema = json.load(f)

    # Schema validation
    schema_good_df, schema_bad_df = validate_schema(df, expected_schema)

    # DQ validation
    dq_good_df, dq_bad_df = apply_dq_rules(schema_good_df)

    # Combine bad data
    final_bad_df = schema_bad_df.unionByName(
        dq_bad_df, allowMissingColumns=True
    )

    # Write outputs
    write_parquet(dq_good_df, bronze_good)
    write_parquet(final_bad_df,bronze_bad)

    records_good = dq_good_df.count()
    records_bad = final_bad_df.count()

    status = (
        "FAILED" if records_good == 0
        else "PARTIAL" if records_bad > 0
        else "SUCCESS"
    )

    end_time = datetime.now(timezone.utc)

    
    return {
        "run_id": run_id,
        "dataset_name": dataset_config["name"],
        "layer": layer,
        "status": status,
        "records_processed": records_read,
        "records_good": records_good,
        "records_bad": records_bad,
        "start_time": start_time,
        "end_time": end_time,
        "environment": env,
        
    }