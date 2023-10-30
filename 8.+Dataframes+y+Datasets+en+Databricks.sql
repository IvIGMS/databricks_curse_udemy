-- Databricks notebook source
-- MAGIC %md # Dataframes y Datasets en Databricks

-- COMMAND ----------

-- MAGIC %md #### Crear DataFrames

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # import pyspark class Row from module sql
-- MAGIC from pyspark.sql import *
-- MAGIC
-- MAGIC # Create Example Data - Departments and Employees
-- MAGIC
-- MAGIC # Create the Departments
-- MAGIC department1 = Row(id='123456', name='Computer Science')
-- MAGIC department2 = Row(id='789012', name='Mechanical Engineering')
-- MAGIC department3 = Row(id='345678', name='Theater and Drama')
-- MAGIC department4 = Row(id='901234', name='Indoor Recreation')
-- MAGIC
-- MAGIC # Create the Employees
-- MAGIC Employee = Row("firstName", "lastName", "email", "salary")
-- MAGIC employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
-- MAGIC employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
-- MAGIC employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
-- MAGIC employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
-- MAGIC employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)
-- MAGIC
-- MAGIC # Create the DepartmentWithEmployees instances from Departments and Employees
-- MAGIC departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
-- MAGIC departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
-- MAGIC departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
-- MAGIC departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])
-- MAGIC
-- MAGIC print(department1)
-- MAGIC print(employee2)
-- MAGIC print(departmentWithEmployees1.employees[0].email)

-- COMMAND ----------

-- MAGIC %md #### Trabajar con DataFrames
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
-- MAGIC df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)
-- MAGIC
-- MAGIC display(df1)
-- MAGIC
-- MAGIC departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
-- MAGIC df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)
-- MAGIC
-- MAGIC display(df2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Unión de dos DataFrames
-- MAGIC
-- MAGIC unionDF = df1.union(df2)
-- MAGIC display(unionDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Escriba el DataFrame unido en un archivo Parquet
-- MAGIC
-- MAGIC # Remove the file if it exists
-- MAGIC dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)
-- MAGIC unionDF.write.parquet("/tmp/databricks-df-example.parquet")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Leer un DataFrame del archivo Parquet
-- MAGIC parquetDF = spark.read.parquet("/tmp/databricks-df-example.parquet")
-- MAGIC display(parquetDF)

-- COMMAND ----------

-- MAGIC %md #### Selección de datos y transformaciones en DataFrames

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Explotar la columna de empleados
-- MAGIC
-- MAGIC from pyspark.sql.functions import explode
-- MAGIC
-- MAGIC explodeDF = unionDF.select(explode("employees").alias("e"))
-- MAGIC flattenDF = explodeDF.selectExpr("e.firstName", "e.lastName", "e.email", "e.salary")
-- MAGIC
-- MAGIC flattenDF.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # filter()para devolver las filas que coinciden con un predicado
-- MAGIC filterDF = flattenDF.filter(flattenDF.firstName == "xiangrui").sort(flattenDF.lastName)
-- MAGIC display(filterDF)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, asc
-- MAGIC
-- MAGIC # Use `|` instead of `or`
-- MAGIC filterDF = flattenDF.filter((col("firstName") == "xiangrui") | (col("firstName") == "michael")).sort(asc("lastName"))
-- MAGIC display(filterDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC whereDF = flattenDF.where((col("firstName") == "xiangrui") | (col("firstName") == "michael")).sort(asc("lastName"))
-- MAGIC display(whereDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Reemplazar nullvalores con el --uso de la función DataFrame Na
-- MAGIC nonNullDF = flattenDF.fillna("--")
-- MAGIC display(nonNullDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Recupere solo filas con datos faltantes en firstName o lastName
-- MAGIC filterNonNullDF = flattenDF.filter(col("firstName").isNull() | col("lastName").isNull()).sort("email")
-- MAGIC display(filterNonNullDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Ejemplos de agregaciones usando agg()ycountDistinct()
-- MAGIC from pyspark.sql.functions import countDistinct
-- MAGIC
-- MAGIC countDistinctDF = nonNullDF.select("firstName", "lastName")\
-- MAGIC   .groupBy("firstName")\
-- MAGIC   .agg(countDistinct("lastName").alias("distinct_last_names"))
-- MAGIC
-- MAGIC display(countDistinctDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Resumen de los salarios
-- MAGIC salarySumDF = nonNullDF.agg({"salary" : "sum"})
-- MAGIC display(salarySumDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC nonNullDF.describe("salary").show()

-- COMMAND ----------

-- MAGIC %md #### Visualización de datos

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Un ejemplo usando pandas y la integración de Matplotlib
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC plt.clf()
-- MAGIC pdDF = nonNullDF.toPandas()
-- MAGIC pdDF.plot(x='firstName', y='salary', kind='bar', rot=45)
-- MAGIC display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # eliminar el archivo Parquet
-- MAGIC dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)