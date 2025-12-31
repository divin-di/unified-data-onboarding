from pyspark.sql.functions import col

def bronze_to_silver(bronze_df):
    """
    Apply deduplication and business-level cleaning
    """
    silver_df = (
        bronze_df
        .dropDuplicates(["txn_id"])
        .filter(col("amount") > 0)
    )

    return silver_df
