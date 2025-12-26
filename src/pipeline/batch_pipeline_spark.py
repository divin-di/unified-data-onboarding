import json
from pyspark.sql import SparkSession
from readers.json_reader_spark import read_json
from validation.schema_validator_spark import validate_schema
from validation.dq_validator import apply_dq_rules
from writers.parquet_writer_spark import write_parquet
from utils.config_loader import load_config

config = load_config("configs/pipeline.yml")

spark = SparkSession.builder \
    .appName(config["spark"]["app_name"]) \
    .getOrCreate()

df = read_json( spark,config["input"]["path"])

with open(config["schema"]["path"]) as f:
    expected_schema = json.load(f)
    
bronze_good_path = "data/output/bronze/good"
bronze_bad_path = "data/output/bronze/bad"



schema_good_df, schema_bad_df = validate_schema(df, expected_schema)
dq_good_df, dq_bad_df = apply_dq_rules(schema_good_df)
final_bad_df = schema_bad_df.unionByName(dq_bad_df, allowMissingColumns=True)

# Write outputs
write_parquet(dq_good_df, config["output"]["bronze"]["good"])
write_parquet(final_bad_df,config["output"]["bronze"]["bad"])

spark.stop()
