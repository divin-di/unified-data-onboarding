import sys
import yaml
import uuid
from pyspark.sql import SparkSession
from pipeline_logic.dataset_processor import process_layer # Renamed to reflect generic use
from observability.run_metadata_writer import write_run_metadata

# 1. Setup Execution Context
run_id = str(uuid.uuid4())
config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/pipeline.yml"
target_layer = sys.argv[2].lower() if len(sys.argv) > 2 else "bronze"

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# Initialize Spark once for the entire session
spark = SparkSession.builder \
    .appName(f"{target_layer.upper()}_Pipeline_{run_id}") \
    .getOrCreate()

all_run_metadata = []
env = config.get("env", "dev")


try:    
    for dataset in config["datasets"]:
        print(f"Processing {dataset['name']}...")
            
            # Call the generic processor
        
        metadata = process_layer(
                    spark=spark,
                    dataset_config=dataset,
                    run_id=run_id,
                    env=env,
                    layer=target_layer
                )
        all_run_metadata.append(metadata)
        print(f"Finished {dataset['name']} - Status: {metadata['status']}")
        
            
            # Safety Check: If a dataset fails in Bronze, you might want to 
            # skip it in Silver. We can check status here.
        if metadata["status"] == "FAILED":
                print(f"Warning: {dataset['name']} failed at {layer} layer.")

    # 3. Finalize Metadata
    write_run_metadata(
        spark,
        all_run_metadata,
        "data/metadata/pipeline_runs"
    )

finally:
    spark.stop()
    print("Pipeline Execution Finished.")