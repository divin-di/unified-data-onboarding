from pyspark.sql import SparkSession
from typing import Optional, Dict
from pyspark.sql.types import StructType

def read_csv(spark: SparkSession, path: str, options: Optional[Dict[str, str]] = None, schema: Optional[StructType] = None):
    """
    Reads a CSV file into a Spark DataFrame with optional schema enforcement.
    """
    reader = spark.read
    
    # Apply schema if provided (best practice for Bronze layer)
    if schema:
        reader = reader.schema(schema)
        
    # Apply options (header, delimiter, inferSchema, etc.)
    if options:
        # Use ** unpacking to pass the entire dictionary at once
        reader = reader.options(**options)
        
    return reader.csv(path)




