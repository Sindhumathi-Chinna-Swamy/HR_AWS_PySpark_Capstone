from pyspark.sql import functions as F

ABSENCE_THRESHOLD = 0.20
SHORT_TENURE_MONTHS = 12


# Calculated employee tenure
def calculate_tenure(df):

    end_date = F.coalesce(
        F.col("termination_date"),
        F.current_date()
    )

    return df.withColumn(
        "tenure_days",
        F.datediff(end_date, F.col("hire_date"))
    ).withColumn(
        "tenure_months",
        F.floor(
            F.months_between(end_date, F.col("hire_date"))
        )
    ).withColumn(
        "employment_status",
        F.when(
            F.col("termination_date").isNull(), "Active"
        ).otherwise("Terminated")
    )


# Enriched attendance records with department details
def create_fact_attendance(attendance, employees):

    df = attendance.join(
        employees.select("employee_id", "department_id"),
        on="employee_id",
        how="inner"
    )

    return df.withColumn(
        "year", F.year("attendance_date")
    ).withColumn(
        "month", F.month("attendance_date")
    ).withColumn(
        "quarter", F.quarter("attendance_date")
    )


# Aggregated attendance at employee and date level
def calculate_daily_attendance(attendance):

    return attendance.groupBy(
        "employee_id",
        "attendance_date"
    ).agg(
        F.sum("hours_worked").alias("daily_hours")
    ).withColumn(
        "attendance_status",
        F.when(
            F.col("daily_hours") == 0, "Absent"
        ).otherwise("Present")
    )

# Calculated employee attendance metrics
def calculate_employee_metrics(daily):

    df = daily.groupBy("employee_id").agg(

        F.count("*").alias("total_recorded_days"),

        F.sum(
            F.when(
                F.col("attendance_status") == "Absent", 1
            ).otherwise(0)
        ).alias("total_absent_days"),

        F.sum(
            F.when(
                F.col("attendance_status") == "Present", 1
            ).otherwise(0)
        ).alias("total_present_days"),

        F.sum("daily_hours").alias("total_hours"),

        F.avg("daily_hours").alias("avg_daily_hours")
    )

    return df.withColumn(
        "absenteeism_rate",
        F.round(
            F.col("total_absent_days") /
            F.col("total_recorded_days"), 4
        )
    )

# Identified employees with high absenteeism
# and potential attrition risk
def calculate_attrition_risk(employees, metrics):

    df = employees.join(
        metrics,
        on="employee_id",
        how="left"
    )

    df = df.withColumn(
        "high_absenteeism",
        F.col("absenteeism_rate") > ABSENCE_THRESHOLD
    )

    return df.withColumn(
        "attrition_risk",
        F.when(
            F.col("employment_status") == "Terminated",
            "Not Applicable"
        ).when(
            F.col("absenteeism_rate").isNull(),
            "Unknown"
        ).when(
            (F.col("absenteeism_rate") > ABSENCE_THRESHOLD) &
            (F.col("tenure_months") < SHORT_TENURE_MONTHS),
            "High"
        ).otherwise("Low")
    )

# Executed all business transformations
def transform_data(employees, departments, attendance):

    dim_employee = calculate_tenure(employees)

    dim_department = departments

    fact_attendance = create_fact_attendance(
        attendance, dim_employee
    )

    daily = calculate_daily_attendance(fact_attendance)

    metrics = calculate_employee_metrics(daily)

    agg_employee_metrics = calculate_attrition_risk(
        dim_employee, metrics
    )

    return {
        "dim_employee": dim_employee,
        "dim_department": dim_department,
        "fact_attendance": fact_attendance,
        "agg_employee_metrics": agg_employee_metrics
    }