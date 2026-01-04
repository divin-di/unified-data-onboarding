def parquet_writer_silver(df, path, mode="overwrite", **options):
   (
        df
        .write
        .mode(mode)
        .options(**options) # This lets you pass partitionBy, compression, etc.
        .parquet(path)
    )
