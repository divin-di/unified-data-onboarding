from pyspark.sql.functions import lit
from pyspark.sql.types import LongType, DoubleType, StringType

def validate_schema(df, expected_schema):
    # 1. Map string labels to actual PySpark Types
    TYPE_MAPPING = {
        "long": LongType,
        "double": DoubleType,
        "string": StringType
    }
    
    errors = []
    
    # 2. Accumulate ALL errors instead of returning immediately
    for col_name, type_label in expected_schema.items():
        if col_name not in df.columns:
            errors.append(f"Missing column: {col_name}")
            continue
            
        actual_type = df.schema[col_name].dataType
        expected_class = TYPE_MAPPING.get(type_label)
        
        if expected_class and not isinstance(actual_type, expected_class):
            errors.append(f"Type mismatch on {col_name}: expected {type_label}, got {actual_type.simpleString()}")

    # 3. Handle the results
    if errors:
        error_msg = " | ".join(errors)
        error_df = df.withColumn("error_reason", lit(error_msg))
        return df.limit(0), error_df  # Return empty valid DF, full error DF
        
    return df, df.limit(0)