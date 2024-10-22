# Databricks notebook source
dbutils.fs.ls('/Volumes/raw/rais/estabelecimentos')

# COMMAND ----------

df=spark.read.format('csv').options(header=True, delimiter=';',encoding='ISO-8859-1').load('/Volumes/raw/rais/estabelecimentos')
df.display()

# COMMAND ----------

df2=spark.read.format('csv').options(header=True, delimiter=';',encoding='ISO-8859-1').load('/Volumes/raw/rais/vinculos')
df2.display()

# COMMAND ----------


