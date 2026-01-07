import sys
import yaml
import uuid
import os
from pyspark.sql import SparkSession

# 1. Setup Environment & Repo Paths
repo_root = "/Workspace/Users/divinmr@gmail.com/unified-data-onboarding"
if repo_root not in sys.path:
    sys.path.append(repo_root)

# Create Widgets (These appear at the top of your Databricks Notebook)
dbutils.widgets.text("config_path", f"{repo_root}/configs/pipeline.yml")
dbutils.widgets.text("layer", "both")
dbutils.widgets.text("dataset", "all")

# 2. Functional Imports
from src.pipeline_logic.dataset_processor import process_layer
from src.observability.run_metadata_writer import write_run_metadata

def run_pipeline():
    spark_sess = SparkSession.builder.getOrCreate()
    run_id = str(uuid.uuid4())
    
    # --- LOGIC: PRIORITIZE WIDGETS (ADF) THEN SYS.ARGV (TERMINAL) ---
    # Get values from Widgets
    w_config = dbutils.widgets.get("config_path")
    w_layer = dbutils.widgets.get("layer")
    w_dataset = dbutils.widgets.get("dataset")

    # Clean terminal arguments for fallback
    clean_args = [arg for arg in sys.argv if not arg.startswith("-f") and "/databricks/" not in arg]

    # Resolve actual values to use
    config_path = w_config if w_config else (clean_args[1] if len(clean_args) > 1 else f"{repo_root}/configs/pipeline.yml")
    target_layer = w_layer.lower() if w_layer else (clean_args[2].lower() if len(clean_args) > 2 else "both")
    filter_dataset = w_dataset if w_dataset else (clean_args[3] if len(clean_args) > 3 else "all")

    # Layer Logic
    layers_to_run = ["bronze", "silver"] if target_layer == "both" else [target_layer]

    # Load Config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Dataset Filtering Logic (Step 2 logic for filtering)
    if filter_dataset != "all":
        config["datasets"] = [ds for ds in config["datasets"] if ds["name"] == filter_dataset]

    all_run_metadata = []
    env = config.get("env", "dev")
   
    try:
        for current_layer in layers_to_run:
            print(f"\n" + "="*40)
            print(f"--- STARTING {current_layer.upper()} LAYER ---")
            print("="*40)
            
            for dataset in config["datasets"]:
                print(f"Processing {dataset['name']} ({current_layer})...")
                
                metadata = process_layer(
                    spark=spark_sess,
                    dataset_config=dataset,
                    run_id=run_id,
                    env=env,
                    layer=current_layer
                )
                all_run_metadata.append(metadata)
                print(f"Finished {dataset['name']} - Status: {metadata['status']}")

        # 3. Finalize Metadata
        write_run_metadata(
            spark_sess,
            all_run_metadata,
            "/mnt/metadata"
        )

    finally:
        print(f"Pipeline Execution Finished for Run ID: {run_id}")

if __name__ == "__main__":
    run_pipeline()