# Databricks notebook source
from pyspark.sql.functions import split,current_timestamp
from pyspark.sql.types import *

# COMMAND ----------

df_municipios=spark.read.format("csv").options(header=True,delimiter=";",encoding='ISO-8859-1').load("/Volumes/raw/rais/municipios/rais_municipios.csv")
df_municipios.display()

# COMMAND ----------

df_municipios_bronze=df_municipios.withColumn("ingestion_date", current_timestamp())
df_municipios_bronze.display()

# COMMAND ----------

df_municipios_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/municipios/') \
.saveAsTable('bronze.rais.municipios')
