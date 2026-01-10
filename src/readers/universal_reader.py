import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType
from src.readers.json_reader_spark import read_json
from src.readers.csv_reader_spark import read_csv

def get_spark_schema(schema_path: str) -> StructType:
    """
    Parses a simple key-value JSON schema into a Spark StructType.
    Example: {"id": "long"} -> StructType([StructField("id", LongType(), True)])
    """
    # Mapping dictionary to translate your JSON strings to Spark Types
    type_map = {
        "long": LongType(),
        "double": DoubleType(),
        "string": StringType(),
        "timestamp": TimestampType()
    }

    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found at: {schema_path}")

    with open(schema_path, 'r') as f:
        schema_dict = json.load(f)

    # Build the schema fields dynamically
    fields = [
        StructField(col_name, type_map.get(col_type.lower(), StringType()), True)
        for col_name, col_type in schema_dict.items()
    ]
    
    return StructType(fields)

def read_input(spark: SparkSession, dataset_cfg: dict):
    """
    Reads data using specific formats, options, and a manually parsed schema.
    """
    # 1. Extract configurations using .get() for safety
    bronze_input = dataset_cfg.get("bronze_layer", {}).get("input", {})
    fmt = bronze_input.get("format")
    path = bronze_input.get("path")
    options = bronze_input.get("options") or {} # Handle NoneType options in YAML
    
    # 2. Parse Schema
    schema_path = dataset_cfg.get("schema", {}).get("path")
    schema = None
    
    if schema_path:
        try:
            schema = get_spark_schema(schema_path)
            print(f"Successfully loaded schema from {schema_path}")
        except Exception as e:
            # Fallback logic if schema fails
            print(f"Warning: Failed to parse schema. Error: {e}")
            options["inferSchema"] = "true" 

    # 3. Routing
    if fmt == "csv":
        return read_csv(spark, path, options=options, schema=schema)
    
    elif fmt == "json":
        return read_json(spark, path, options=options, schema=schema)
    
    else:
        raise ValueError(f"Unsupported format: {fmt}. Check your YAML configuration.")