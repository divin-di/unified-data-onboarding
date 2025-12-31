def write_parquet(df, path, mode="append", **options):
    """
    Universal writer. Handles mode and extra Spark options like partitioning.
    """
    (
        df
        .write
        .mode(mode)
        .options(**options) # This lets you pass partitionBy, compression, etc.
        .parquet(path)
    )