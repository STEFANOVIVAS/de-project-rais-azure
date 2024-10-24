# Databricks notebook source
df_estabelecimentos=spark.read.format('csv').options(header=True, delimiter=';',encoding='ISO-8859-1').load('/Volumes/raw/rais/estabelecimentos')
df_estabelecimentos.display()

# COMMAND ----------

def replace_blank_for_underscore(dataframe):
    renamed_columns=[col.replace(" ", "_") for col in dataframe.columns]
    dataframe=dataframe.toDF(*renamed_columns)
    return dataframe
    
df_estabelecimentos=replace_blank_for_underscore(df_estabelecimentos)

# COMMAND ----------

df_estabelecimentos_bronze=df_estabelecimentos.withColumn("ingestion_date", current_timestamp()).withColumn("source", lit("raw_layer/estabelecimentos/RAIS_ESTAB_PUB.txt"))

# COMMAND ----------

df_estabelecimentos_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/estabelecimentos/') \
.saveAsTable('bronze.rais.estabelecimentos')
