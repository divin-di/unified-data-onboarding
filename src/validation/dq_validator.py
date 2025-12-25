from pyspark.sql.functions import col, when

def apply_dq_rules(df):
    df_with_errors = df.withColumn(
        "error_reason",
        when(col("amount") < 0, "Negative amount")
        .when(col("txn_id").isNull(), "Missing txn_id")
        .otherwise(None)
    )

    valid_df = df_with_errors.filter(col("error_reason").isNull()) \
                             .drop("error_reason")

    invalid_df = df_with_errors.filter(col("error_reason").isNotNull())

    return valid_df, invalid_df
