import json
from pyspark.sql import SparkSession
from readers.json_reader_spark import read_json
from validation.schema_validator_spark import validate_schema
from validation.dq_validator import apply_dq_rules
from writers.parquet_writer_spark import write_parquet



spark = SparkSession.builder \
    .appName("UnifiedBatchIngestion") \
    .getOrCreate()

input_path = "data/input/binance_events.json"
bronze_good_path = "data/output/bronze/good"
bronze_bad_path = "data/output/bronze/bad"

with open("schemas/transaction_schema.json") as f:
    expected_schema = json.load(f)

df = read_json(spark,input_path)
schema_good_df, schema_bad_df = validate_schema(df, expected_schema)
dq_good_df, dq_bad_df = apply_dq_rules(schema_good_df)
final_bad_df = schema_bad_df.unionByName(dq_bad_df, allowMissingColumns=True)

# Write outputs
write_parquet(dq_good_df, bronze_good_path)
write_parquet(final_bad_df, bronze_bad_path)
