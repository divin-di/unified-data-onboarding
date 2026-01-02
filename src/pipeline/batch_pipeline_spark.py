import json
import sys
import uuid
import yaml
from pyspark.sql import SparkSession
from readers.json_reader_spark import read_json
from validation.schema_validator_spark import validate_schema
from validation.dq_validator import apply_dq_rules
from writers.parquet_writer_spark import write_parquet
from utils.config_loader import load_config
from observability.run_metadata_writer import write_run_metadata
from datetime import datetime, timezone


run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

if len(sys.argv) > 1:
    config_path = sys.argv[1]
else:
    config_path = "configs/pipeline.yml" # local fallback

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

spark = SparkSession.builder \
    .appName(config["bronze_layer"]["spark_app_name"]) \
    .getOrCreate()

df = read_json( spark,config["bronze_layer"]["input"]["path"])
records_read = df.count()

with open(config["schema"]["path"]) as f:
    expected_schema = json.load(f)
    
bronze_good_path = "data/output/bronze/good"
bronze_bad_path = "data/output/bronze/bad"



schema_good_df, schema_bad_df = validate_schema(df, expected_schema)
dq_good_df, dq_bad_df = apply_dq_rules(schema_good_df)
final_bad_df = schema_bad_df.unionByName(dq_bad_df, allowMissingColumns=True)


# Write outputs
write_parquet(dq_good_df, config["bronze_layer"]["output"]["good"])
write_parquet(final_bad_df,config["bronze_layer"]["output"]["bad"])

records_good = dq_good_df.count()
records_bad = final_bad_df.count()

    
status = (
    "FAILED" if records_good == 0
    else "PARTIAL" if records_bad > 0
    else "SUCCESS"
)

end_time = datetime.now(timezone.utc)

metadata = {
    "run_id": run_id,
    "pipeline_name": "binance_batch",
    "environment": config["env"],
    "start_time": start_time,
    "end_time": end_time,
    "status": status,
    "records_read": records_read,
    "records_good": records_good,
    "records_bad": records_bad,
    "config_path": config_path
}

write_run_metadata(
    spark,
    metadata,
    "data/metadata/pipeline_runs"
)

spark.stop()
