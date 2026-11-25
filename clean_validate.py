from pyspark.sql.functions import col, trim, initcap, to_date
from pyspark.sql.functions import expr, current_date, isnan

# Validated required columns
def validate_schema(df, required_columns):

    missing = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return df


# Removed records with missing values
def remove_nulls(df, columns):

    return df.dropna(subset=columns)


# Removed duplicate records
def remove_duplicates(df, columns):

    return df.dropDuplicates(columns)


# Normalized text values
def normalize_text(df, column):

    return df.withColumn(
        column,
        initcap(trim(col(column)))
    )


# Converted timestamp or string into date
def convert_date(df, column):

    return df.withColumn(
        column,
        to_date(col(column))
    )


# Detected orphan records
def detect_orphans(df, reference_df, key):

    return df.join(
        reference_df.select(key).distinct(),
        on=key,
        how="left_anti"
    )


# Removed orphan records
def remove_orphans(df, reference_df, key):

    return df.join(
        reference_df.select(key).distinct(),
        on=key,
        how="left_semi"
    )


# Converted columns into their required data types
def cast_columns(df, column_types):

    for column, datatype in column_types.items():

        df = df.withColumn(
            column,
            expr(f"try_cast(`{column}` as {datatype})")
        )

    return df


# Removed exact duplicates and conflicting primary keys
def clean_primary_key(df, key):

    df = remove_duplicates(df, df.columns)

    conflicts = df.groupBy(key).count().filter(
        col("count") > 1
    )

    return df.join(
        conflicts.select(key),
        on=key,
        how="left_anti"
    )


# Cleaned and validated department records
def clean_departments(df):

    required = [
        "department_id",
        "department_name"
    ]

    df = validate_schema(df, required)

    df = cast_columns(df, {
        "department_id": "int"
    })

    df = remove_nulls(df, required)

    df = normalize_text(df, "department_name")

    df = df.filter(
        (col("department_id") > 0) &
        (col("department_name") != "")
    )

    df = clean_primary_key(df, "department_id")

    return df

# Cleaned and validated employee records
def clean_employees(df, departments):

    required = [
        "employee_id",
        "employee_name",
        "department_id",
        "hire_date",
        "termination_date",
        "gender"
    ]

    df = validate_schema(df, required)

    # Identified supplied termination dates
    df = df.withColumn(
        "_has_termination",
        col("termination_date").isNotNull() &
        (trim(col("termination_date").cast("string")) != "")
    )

    df = cast_columns(df, {
        "employee_id": "int",
        "department_id": "int",
        "hire_date": "date",
        "termination_date": "date"
    })

    df = remove_nulls(df, [
        "employee_id",
        "employee_name",
        "department_id",
        "hire_date"
    ])

    df = normalize_text(df, "employee_name")

    # Removed invalid employee records
    df = df.filter(
        (col("employee_id") > 0) &
        (col("employee_name") != "") &
        (col("hire_date") <= current_date())
    )

    # Validated termination dates
    df = df.filter(
        (~col("_has_termination")) |
        (
            col("termination_date").isNotNull() &
            (col("termination_date") >= col("hire_date")) &
            (col("termination_date") <= current_date())
        )
    ).drop("_has_termination")

    df = clean_primary_key(df, "employee_id")

    # Removed employees with unknown departments
    df = remove_orphans(
        df, departments, "department_id"
    )

    return df


# Cleaned and validated attendance records
def clean_attendance(df, employees):

    required = [
        "attendance_id",
        "employee_id",
        "attendance_date",
        "hours_worked"
    ]

    df = validate_schema(df, required)

    df = cast_columns(df, {
        "attendance_id": "int",
        "employee_id": "int",
        "attendance_date": "date",
        "hours_worked": "double"
    })

    df = remove_nulls(df, required)

    # Removed invalid attendance records
    df = df.filter(
        (col("attendance_id") > 0) &
        (~isnan(col("hours_worked"))) &
        (col("hours_worked") >= 0) &
        (col("hours_worked") <= 24) &
        (col("attendance_date") <= current_date())
    )

    df = clean_primary_key(df, "attendance_id")

    # Removed attendance with unknown employees
    df = remove_orphans(
        df, employees, "employee_id"
    )

    # Validated attendance against employment period
    df = df.join(
        employees.select(
            "employee_id",
            "hire_date",
            "termination_date"
        ),
        on="employee_id",
        how="inner"
    )

    df = df.filter(
        (col("attendance_date") >= col("hire_date")) &
        (
            col("termination_date").isNull() |
            (col("attendance_date") <= col("termination_date"))
        )
    )

    return df.drop("hire_date", "termination_date")

if __name__ == "__main__":

    from config import create_spark
    from extract import extract_data

    spark = create_spark()

    employees, departments, attendance = extract_data(spark)

    departments_clean = clean_departments(departments)

    employees_clean = clean_employees(
        employees, departments_clean
    )

    attendance_clean = clean_attendance(
        attendance, employees_clean
    )

    # Displayed data quality metrics
    datasets = [
        ("Departments", departments, departments_clean),
        ("Employees", employees, employees_clean),
        ("Attendance", attendance, attendance_clean)
    ]

    for name, raw, clean in datasets:

        raw_count = raw.count()
        clean_count = clean.count()

        print(f"\n{name}")
        print("Raw records:", raw_count)
        print("Clean records:", clean_count)
        print("Removed records:", raw_count - clean_count)

    spark.stop()