from config import PROCESSED_PATH


# Created a reusable function to write Parquet files to S3
def write_parquet(df, table_name):

    path = PROCESSED_PATH + table_name + "/"

    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(path)

    print(f"Successfully loaded: {table_name}")
    print(f"S3 Path: {path}")


# Loaded all transformed datasets into S3
def load_data(tables):

    for table_name, df in tables.items():

        write_parquet(df, table_name)

    print("\nAll datasets successfully loaded into S3!")