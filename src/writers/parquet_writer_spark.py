def write_parquet(df, path):
    (
        df
        .write
        .mode("append")
        .parquet(path)
    )
