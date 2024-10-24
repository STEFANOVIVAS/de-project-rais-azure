# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Set schema and load data

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,current_timestamp,lit

# COMMAND ----------

df_vinculos=spark.read.format("csv").options(header=True,delimiter=";",encoding='ISO-8859-1').load("/Volumes/raw/rais/vinculos/*.txt")

# COMMAND ----------

def replace_blank_for_underscore(dataframe):
    renamed_columns=[col.replace(" ", "_") for col in dataframe.columns]
    dataframe=dataframe.toDF(*renamed_columns)
    return dataframe
    
df_vinculos=replace_blank_for_underscore(df_vinculos)


# COMMAND ----------

def replace_parentheses_for_underscore(dataframe):
    renamed_columns=[col.replace("(SM)", "SM") for col in dataframe.columns]
    dataframe=dataframe.toDF(*renamed_columns)
    return dataframe
    
df_vinculos_bronze=replace_parentheses_for_underscore(df_vinculos)

# COMMAND ----------

df_vinculos_bronze=df_vinculos_bronze.withColumn("ingestion_date", current_timestamp()).withColumn("source", lit("raw_layer/vinculos/*.txt"))

# COMMAND ----------

df_vinculos_bronze.columns

# COMMAND ----------

df_vinculos_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/vinculos/') \
.saveAsTable('bronze.rais.vinculos')
