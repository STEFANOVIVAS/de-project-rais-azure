# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit


# COMMAND ----------

df_municipios=spark.read.format("csv").options(header=True,delimiter=";",encoding='ISO-8859-1').load("/Volumes/raw/rais/municipios/rais_municipios.csv")


# COMMAND ----------

df_municipios_bronze=df_municipios.withColumn("ingestion_date", current_timestamp()).withColumn("source", lit("raw_layer/municipios/rais_municipios.csv"))


# COMMAND ----------

df_municipios_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true")\
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/municipios/') \
.saveAsTable('bronze.rais.municipios')
