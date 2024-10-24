# Databricks notebook source
from pyspark.sql.functions import split,current_timestamp,lit
from pyspark.sql.types import *

# COMMAND ----------

df_ocupacao=spark.read.format("csv").options(header=True,delimiter=";",encoding='ISO-8859-1').load("/Volumes/raw/rais/ocupacao/rais_ocupacao.csv")


# COMMAND ----------

df_ocupacao_bronze=df_ocupacao.withColumn("ingestion_date", current_timestamp()).withColumnRenamed("CBO 2002 Ocupação", "CBO_2002_Ocupação").withColumn("source", lit("raw_layer/ocupacao/rais_ocupacao.csv"))


# COMMAND ----------

df_ocupacao_bronze.display()

# COMMAND ----------

df_ocupacao_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/ocupacao/') \
.saveAsTable('bronze.rais.ocupacao')
