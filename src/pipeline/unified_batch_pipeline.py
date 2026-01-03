from pyspark.sql import SparkSession
from pipeline_logic.dataset_processor import process_dataset
from observability.run_metadata_writer import write_run_metadata
import json
import yaml
import uuid
import sys


run_id = str(uuid.uuid4())

if len(sys.argv) > 1:
    config_path = sys.argv[1]
else:
    config_path = "configs/pipeline.yml" # local fallback

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

spark = SparkSession.builder \
    .appName(config["datasets"][0]["bronze_layer"]["spark_app_name"]) \
    .getOrCreate()

all_run_metadata = []

for dataset in config["datasets"]:
    metadata = process_dataset(
        spark=spark,
        dataset_config=dataset,
        run_id=run_id,
        env=config["env"]
    )
    all_run_metadata.append(metadata)

# Write run metadata (one row per dataset)
write_run_metadata(
    spark,
    all_run_metadata,
    "data/metadata/pipeline_runs"
)

spark.stop()
