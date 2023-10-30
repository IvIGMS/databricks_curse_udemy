# Databricks notebook source
# MAGIC %md
# MAGIC # Fundamentos de Apache Spark: SQL/DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark SQLtrabaja con DataFrames**. Un DataFrame es una **representación relacional de los datos**. Proporciona funciones con capacidades similares a SQL. Además, permite escribir **consultas tipo SQL** para nuestro análisis de datos.
# MAGIC
# MAGIC Los DataFrames son similares a las tablas relacionales o DataFrames en Python / R auqnue con muchas optimizaciones que se ejecutan de manera "oculta" para el usuario. Hay varias formas de crear DataFrames a partir de colecciones, tablas HIVE, tablas relacionales y RDD.

# COMMAND ----------

import findspark
findspark.init()

import pandas as pd
import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crear la sesión de Spark 

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crear el DataFrame

# COMMAND ----------

emp = [(1, "AAA", "dept1", 1000),
    (2, "BBB", "dept1", 1100),
    (3, "CCC", "dept1", 3000),
    (4, "DDD", "dept1", 1500),
    (5, "EEE", "dept2", 8000),
    (6, "FFF", "dept2", 7200),
    (7, "GGG", "dept3", 7100),
    (8, "HHH", "dept3", 3700),
    (9, "III", "dept3", 4500),
    (10, "JJJ", "dept5", 3400)]

dept = [("dept1", "Department - 1"),
        ("dept2", "Department - 2"),
        ("dept3", "Department - 3"),
        ("dept4", "Department - 4")

       ]

df = spark.createDataFrame(emp, ["id", "name", "dept", "salary"])

deptdf = spark.createDataFrame(dept, ["id", "name"]) 

# COMMAND ----------

df.show()

# COMMAND ----------

#Crear un df a partir de una tabla de Hive
df = spark.table(“tbl_name”)

# COMMAND ----------

# MAGIC %md
# MAGIC # Operaciones básicas en DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC ### count
# MAGIC * Cuenta el número de filas

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### columns

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### dtypes
# MAGIC ** Accede al DataType de columnas dentro del DataFrame

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### schema
# MAGIC ** Comprueba cómo Spark almacena el esquema del DataFrame

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### printSchema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### select
# MAGIC * Seleccione columnas del DataFrame

# COMMAND ----------

df.select("id", "name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filter
# MAGIC
# MAGIC * Filtrar las filas según alguna condición.
# MAGIC * Intentemos encontrar las filas con id = 1.
# MAGIC * Hay diferentes formas de especificar la condición.

# COMMAND ----------

df.filter(df["id"] == 1).show()
df.filter(df.id == 1).show()

# COMMAND ----------

df.filter(col("id") == 1).show()
df.filter("id = 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop
# MAGIC * Elimina una columna en particular

# COMMAND ----------

newdf = df.drop("id")
newdf.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregations
# MAGIC * Podemos usar la función groupBy para agrupar los datos y luego usar la función "agg" para realizar la agregación de datos agrupados.

# COMMAND ----------

(df.groupBy("dept")
    .agg(
        count("salary").alias("count"),
        sum("salary").alias("sum"),
        max("salary").alias("max"),
        min("salary").alias("min"),
        avg("salary").alias("avg")
        ).show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sorting
# MAGIC
# MAGIC * Ordena los datos según el "salario". De forma predeterminada, la clasificación se realizará en orden ascendente.

# COMMAND ----------

df.sort("salary").show(5)

# COMMAND ----------

# Sort the data in descending order.
df.sort(desc("salary")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columnas derivadas
# MAGIC * Podemos usar la función "withColumn" para derivar la columna en función de las columnas existentes ...

# COMMAND ----------

df.withColumn("bonus", col("salary") * .1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins
# MAGIC
# MAGIC * Podemos realizar varios tipos de combinaciones en múltiples DataFrames.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Sin%20t%C3%ADtulo.png](attachment:Sin%20t%C3%ADtulo.png)

# COMMAND ----------

# Inner JOIN.
df.join(deptdf, df["dept"] == deptdf["id"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Outer Join

# COMMAND ----------

df.join(deptdf, df["dept"] == deptdf["id"], "left_outer").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Outer Join

# COMMAND ----------

df.join(deptdf, df["dept"] == deptdf["id"], "right_outer").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Outer Join

# COMMAND ----------

df.join(deptdf, df["dept"] == deptdf["id"], "outer").show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Consultas SQL
# MAGIC * Ejecución de consultas tipo SQL.
# MAGIC * También podemos realizar análisis de datos escribiendo consultas similares a SQL. Para realizar consultas similares a SQL, necesitamos registrar el DataFrame como una Vista temporal.

# COMMAND ----------

# Register DataFrame as Temporary Table
df.createOrReplaceTempView("temp_table")

# Execute SQL-Like query.
spark.sql("select * from temp_table where id = 1").show()

# COMMAND ----------

spark.sql("select distinct id from temp_table").show(10)

# COMMAND ----------

spark.sql("select * from temp_table where salary >= 1500").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leyendo la tabla HIVE como DataFrame

# COMMAND ----------

# DB_NAME : Name of the the HIVE Database
# TBL_NAME : Name of the HIVE Table


df = spark.table("DB_NAME"."TBL_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar DataFrame como tabla HIVE

# COMMAND ----------

df.write.saveAsTable("DB_NAME.TBL_NAME")

## También podemos seleccionar el argumento "modo" con overwrite", "append", "error" etc.
df.write.saveAsTable("DB_NAME.TBL_NAME", mode="overwrite")

# De forma predeterminada, la operación guardará el DataFrame como una tabla interna / administrada de HIVE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar el DataFrame como una tabla externa HIVE

# COMMAND ----------

df.write.saveAsTable("DB_NAME.TBL_NAME", path=<location_of_external_table>)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crea un DataFrame a partir de un archivo CSV
# MAGIC * Podemos crear un DataFrame usando un archivo CSV y podemos especificar varias opciones como un separador, encabezado, esquema, inferSchema y varias otras opciones.

# COMMAND ----------

 df = spark.read.csv("path_to_csv_file", sep="|", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar un DataFrame como un archivo CSV

# COMMAND ----------

df.write.csv("path_to_CSV_File", sep="|", header=True, mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crea un DataFrame a partir de una tabla relacional
# MAGIC * Podemos leer los datos de bases de datos relacionales usando una URL JDBC.

# COMMAND ----------

# url : a JDBC URL of the form jdbc:subprotocol:subname
# TBL_NAME : Name of the relational table.
# USER_NAME : user name to connect to DataBase.
# PASSWORD: password to connect to DataBase.


relational_df = spark.read.format('jdbc')
                        .options(url=url, dbtable= <TBL_NAME>, user= <USER_NAME>, password = <PASSWORD>)
                        .load()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar el DataFrame como una tabla relacional
# MAGIC * Podemos guardar el DataFrame como una tabla relacional usando una URL JDBC.

# COMMAND ----------

# url : a JDBC URL of the form jdbc:subprotocol:subname
# TBL_NAME : Name of the relational table.
# USER_NAME : user name to connect to DataBase.
# PASSWORD: password to connect to DataBase.


 relational_df.write.format('jdbc')
                    .options(url=url, dbtable= <TBL_NAME>, user= <USER_NAME>, password = <PASSWORD>)
                    .mode('overwrite')
                    .save()