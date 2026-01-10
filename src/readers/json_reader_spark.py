#from pyspark.sql import SparkSession

#def read_json(spark: SparkSession, path: str):
 #   return spark.read.option("multiline", "true").json(path)


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Optional, Dict

def read_json(spark: SparkSession, path: str, options: Optional[Dict[str, str]] = None, schema: Optional[StructType] = None):
    """
    Reads a JSON file into a Spark DataFrame with optional schema and configuration.
    """
    reader = spark.read
    
    # 1. Apply Schema if provided
    if schema:
        reader = reader.schema(schema)
        
    # 2. Apply Options (e.g., multiline, allowComments, etc.)
    # We set multiline to true by default but allow options to override it
    final_options = {"multiline": "true"}
    if options:
        final_options.update(options)
        
    reader = reader.options(**final_options)
        
    return reader.json(path)