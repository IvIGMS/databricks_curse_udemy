# Databricks notebook source
# Inicializa la sesión de Spark si no se ha hecho antes
# El método enableHiveSupport() es crucial para interactuar con las tablas de Hive en Databricks.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('HiveExample').enableHiveSupport().getOrCreate()

# Crea una tabla de Hive
spark.sql("""
CREATE TABLE IF NOT EXISTS empleados (
    id INT,
    nombre STRING,
    edad INT,
    salario FLOAT
)
LOCATION 'dbfs:/FileStore/tables/hive/empleados'
""")

# Si tienes datos en un DataFrame de Spark, también puedes guardar ese DataFrame como una tabla de Hive
# df.write.saveAsTable('empleados')

# COMMAND ----------

# Inserta algunos datos en la tabla de Hive
spark.sql("""
INSERT INTO empleados VALUES
(1, 'Juan', 30, 50000),
(2, 'Maria', 40, 60000),
(3, 'Luis', 25, 45000)
""")

# COMMAND ----------

# Consulta la tabla de Hive y obtén un DataFrame de Spark
df = spark.sql("SELECT * FROM empleados")

# Muestra el contenido del DataFrame
display(df)

# COMMAND ----------

# Consulta la ubicación de la tabla en DBFS
location = spark.sql("DESCRIBE FORMATTED empleados").filter("col_name == 'Location'").collect()[0]['data_type']
print(location)


# COMMAND ----------

