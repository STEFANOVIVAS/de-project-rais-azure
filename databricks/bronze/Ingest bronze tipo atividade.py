# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit


# COMMAND ----------

df_tipo_atividade=spark.read.format("csv").options(header=True,delimiter=";",encoding='ISO-8859-1').load("/Volumes/raw/rais/tipo_atividade/tipo_atividade.csv")

# COMMAND ----------

df_tipo_atividade_bronze=df_tipo_atividade.withColumn("ingestion_date", current_timestamp()).withColumnRenamed("CNAE 2.0 Subclas","CNAE_2_Subclasses").withColumn("source", lit("raw_layer/tipo_atividade/tipo_atividade.csv"))


# COMMAND ----------

df_tipo_atividade_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/tipo_atividade/') \
.saveAsTable('bronze.rais.tipo_atividade')
