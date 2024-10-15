# Databricks notebook source
# MAGIC %pip install py7zr
# MAGIC import py7zr
# MAGIC
# MAGIC landing_zone = '/mnt/raispipeline/rais/2021/raw_layer/landing_zone/'
# MAGIC raw_layer = '/mnt/raispipeline/rais/2021/raw_layer/'
# MAGIC
# MAGIC for file in dbutils.fs.ls(landing_zone):
# MAGIC         try:
# MAGIC             archive = py7zr.SevenZipFile(f"/dbfs{landing_zone}{file.name}", mode='r')
# MAGIC             archive.extractall(path=f"/dbfs{raw_layer}")
# MAGIC             archive.close()
# MAGIC         except:
# MAGIC             print(f"Não foi possível extrair o arquivo {file.name}")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
