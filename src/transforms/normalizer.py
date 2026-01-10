from pyspark.sql.functions import col

def normalize(df, mappings):
    for src, target in mappings.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, target)
    return df
