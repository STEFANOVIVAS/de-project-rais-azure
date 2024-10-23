# Databricks notebook source
from pyspark.sql.functions import split
from pyspark.sql.types import *

# COMMAND ----------

df_municipios = spark.read.format("delta").table("bronze.rais.municipios")
df_municipios.display()

# COMMAND ----------

df_municipios_split=df_municipios.withColumn("codigo_municipio",split(df_municipios["Município"],":")[0]).withColumn("estado_municipio",split(df_municipios["Município"],":")[1])
df_municipios_split.display()

# COMMAND ----------

df_municipios_split_estado=df_municipios_split.withColumn("estado",split(df_municipios_split["estado_municipio"],"-")[0]).withColumn("municipio",split(df_municipios_split["estado_municipio"],"-")[1])
df_municipios_split_estado.display()

# COMMAND ----------

df_municipios_bronze=df_municipios_split_estado.select("codigo_municipio","estado","municipio")
df_municipios_bronze.display()

# COMMAND ----------

df_municipios_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/municipios/') \
.saveAsTable('bronze.rais.municipios')

# COMMAND ----------


