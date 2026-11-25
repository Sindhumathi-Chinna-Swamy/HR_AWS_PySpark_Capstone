from config import create_spark, PROCESSED_PATH
from pyspark.sql.functions import col


# Created a reusable function to read Parquet files
def read_parquet(spark, table_name):

    return spark.read.parquet(
        PROCESSED_PATH + table_name + "/"
    )


# Created a reusable function to verify data quality
def check_quality(name, df):

    count = df.count()

    status = "PASSED" if count == 0 else "FAILED"

    print(f"{name}: {status} | Invalid records: {count}")


def verify_data(spark):

    # Retrieved transformed Parquet datasets from S3
    employees = read_parquet(spark, "dim_employee")
    departments = read_parquet(spark, "dim_department")
    attendance = read_parquet(spark, "fact_attendance")
    metrics = read_parquet(spark, "agg_employee_metrics")

    # Verified record counts
    print("\n===== RECORD COUNTS =====")

    tables = {
        "dim_employee": employees,
        "dim_department": departments,
        "fact_attendance": attendance,
        "agg_employee_metrics": metrics
    }

    for name, df in tables.items():

        print(f"{name}: {df.count()}")

    # Verified data quality
    print("\n===== DATA QUALITY CHECKS =====")

    check_quality(
        "Negative attendance hours",
        attendance.filter(col("hours_worked") < 0)
    )

    check_quality(
        "Negative employee tenure",
        employees.filter(col("tenure_days") < 0)
    )

    check_quality(
        "Orphan employees",
        employees.join(
            departments,
            "department_id",
            "left_anti"
        )
    )

    check_quality(
        "Orphan attendance records",
        attendance.join(
            employees,
            "employee_id",
            "left_anti"
        )
    )

    check_quality(
        "Duplicate employee IDs",
        employees.groupBy("employee_id")
        .count().filter(col("count") > 1)
    )

    check_quality(
        "Invalid absenteeism rate",
        metrics.filter(
            (col("absenteeism_rate") < 0) |
            (col("absenteeism_rate") > 1)
        )
    )


if __name__ == "__main__":

    spark = create_spark()

    try:
        verify_data(spark)
    finally:
        spark.stop()