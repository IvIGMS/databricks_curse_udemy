# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# Inicializa una sesión de Spark
spark = SparkSession.builder.appName('EjemploPersist').getOrCreate()

# Suponiendo que tienes un DataFrame df que ha sido leído de una fuente de datos
df = spark.read.csv('/FileStore/tables/people.csv', header=True, inferSchema=True)

# Aplica algunas transformaciones computacionalmente intensivas
df_transformado = df.withColumnRenamed('nombre', 'nombre_completo').filter(df['edad'] > 25)

# Persiste el DataFrame transformado en memoria y en disco
df_transformado.persist(StorageLevel.MEMORY_AND_DISK)

# Ahora, cada vez que realices una acción en df_transformado, 
# Spark no tendrá que recalcular las transformaciones desde el principio.
# Por ejemplo, contar el número de filas:
num_filas = df_transformado.count()

# O calcular alguna estadística:
edad_media = df_transformado.agg({'edad': 'mean'}).collect()

# Cuando hayas terminado con df_transformado, es una buena práctica despersistirlo para liberar recursos.
df_transformado.unpersist()
display(df_transformado)
