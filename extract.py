from config import create_spark, RAW_PATH


# Created a reusable function to read CSV files
def read_csv(spark, filename):

    return spark.read.csv(
        RAW_PATH + filename,
        header=True,
        inferSchema=True
    )


# Extracted all three datasets from S3
def extract_data(spark):

    employees = read_csv(spark, "employees.csv")
    departments = read_csv(spark, "departments.csv")
    attendance = read_csv(spark, "attendance.csv")

    return employees, departments, attendance


# Created a reusable function to inspect datasets
def inspect_data(df, name):

    print(f"\nDataset: {name}")
    print("Total Records:", df.count())

    df.show(5)
    df.printSchema()


# Tested data extraction
if __name__ == "__main__":

    spark = create_spark()

    employees, departments, attendance = extract_data(spark)

    inspect_data(employees, "Employees")
    inspect_data(departments, "Departments")
    inspect_data(attendance, "Attendance")

    spark.stop()