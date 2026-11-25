from pyspark.sql import SparkSession

BUCKET = "hr-capstone-pyspark-2026"

RAW_PATH = f"s3a://{BUCKET}/raw/"
PROCESSED_PATH = f"s3a://{BUCKET}/processed/"


# Created Spark session with AWS S3 connectivity
def create_spark():

    spark = SparkSession.builder \
        .appName("HR_AWS_PySpark_Capstone") \
        .master("local[*]") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ) \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        ) \
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.us-east-1.amazonaws.com"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# Tested the S3 connection
if __name__ == "__main__":

    spark = create_spark()

    df = spark.read.csv(
        RAW_PATH + "employees.csv",
        header=True,
        inferSchema=True
    )

    df.show(5)

    print("S3 connection successful!")

    spark.stop()