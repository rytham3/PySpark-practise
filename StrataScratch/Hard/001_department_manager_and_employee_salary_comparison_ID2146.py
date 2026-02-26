from pyspark.sql.functions import avg, col, round as round_

managers = employee_o.filter(col("employee_title") == "Manager") \
        .select(
            col("id").alias("manager_id"),
            col("salary").alias("manager_salary")
            )
            
avg_salary = employee_o.filter(col("employee_title") != "Manager") \
    .groupby("department") \
    .agg(round_(avg("salary")).alias("avg_employee_salary"))
    
df = employee_o.join(
    managers,
    employee_o.manager_id == managers.manager_id,
    "left"
    )
    
df = df.join(avg_salary, on="department", how="left")

result = df.select(
    col("department"),
    col("id").alias("employee_id"),
    col("salary").alias("employee_salary"),
    col("manager_salary"),
    col("avg_employee_salary")
    ).orderBy(
        col("department"),
        col("employee_salary").desc()
        )
        
result.show()
