# Databricks notebook source
from pyspark.sql import SparkSession

# Iniciar una sesión Spark
spark = SparkSession.builder \
    .appName("Crear y Guardar DataFrame en Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Crear un DataFrame con dos columnas y 25 registros
data = [("Alice", 34),
        ("Bob", 45),
        ("Catherine", 29),
        ("David", 31),
        ("Emily", 25),
        ("Frank", 54),
        ("Grace", 48),
        ("Hannah", 21),
        ("Ian", 36),
        ("Jack", 28),
        ("Karen", 39),
        ("Leo", 52),
        ("Maggie", 44),
        ("Nina", 47),
        ("Oscar", 30),
        ("Paul", 50),
        ("Quincy", 41),
        ("Rita", 33),
        ("Steve", 40),
        ("Tracy", 37),
        ("Ursula", 26),
        ("Victor", 43),
        ("Wendy", 55),
        ("Xena", 46),
        ("Yara", 35)]

columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Guardar el DataFrame como una tabla de Hive
df.write.saveAsTable("another_example")