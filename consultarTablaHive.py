# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

df = spark.sql("INSERT INTO empleados VALUES (4, 'Pedro', 29, 3000)")

# COMMAND ----------

df = spark.sql("SELECT * FROM empleados ORDER BY id desc")
display(df)