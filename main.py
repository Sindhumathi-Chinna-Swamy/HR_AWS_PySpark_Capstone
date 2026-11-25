from config import create_spark
from extract import extract_data
from clean_validate import (
    clean_departments,
    clean_employees,
    clean_attendance
)
from transform import transform_data
from load import load_data


# Executed the complete HR ETL pipeline
def main():

    spark = create_spark()

    try:

        # Extracted raw CSV files from S3
        print("\n===== EXTRACTION =====")

        employees, departments, attendance = extract_data(spark)

        print("Extraction completed")

        # Cleaned and validated datasets
        print("\n===== VALIDATION & CLEANING =====")

        departments_clean = clean_departments(departments)

        employees_clean = clean_employees(
            employees, departments_clean
        )

        attendance_clean = clean_attendance(
            attendance, employees_clean
        )

        print("Cleaning completed")

        # Performed business transformations
        print("\n===== TRANSFORMATION =====")

        tables = transform_data(
            employees_clean,
            departments_clean,
            attendance_clean
        )

        print("Transformation functions applied")

        # Loaded transformed Parquet files into S3
        print("\n===== LOADING =====")

        load_data(tables)

        print("\n===== ETL PIPELINE SUCCESSFUL =====")

    except Exception as error:

        print(f"Pipeline failed: {error}")
        raise

    finally:

        spark.stop()


if __name__ == "__main__":

    main()