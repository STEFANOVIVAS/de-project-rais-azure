# Databricks notebook source
# MAGIC %pip install py7zr
# MAGIC import py7zr
# MAGIC
# MAGIC # Estabelecimentos file
# MAGIC landing_zone_estabelecimentos = 'mnt/raispipeline/rais/2021/raw_layer/estabelecimentos'
# MAGIC try:
# MAGIC     archive = py7zr.SevenZipFile(f"/dbfs/{landing_zone_estabelecimentos}/compactado/RAIS_ESTAB_PUB.7z", mode='r')
# MAGIC     archive.extractall(path=f"/dbfs/{landing_zone_estabelecimentos}/")
# MAGIC     archive.close()
# MAGIC except:
# MAGIC             print(f"Não foi possível extrair o arquivo RAIS_ESTAB_PUB.7z")
# MAGIC
# MAGIC
# MAGIC #Vinculos files
# MAGIC landing_zone_vinculos = 'mnt/raispipeline/rais/2021/raw_layer/vinculos'
# MAGIC
# MAGIC for file in dbutils.fs.ls(f"/{landing_zone_vinculos}/compactado/"):
# MAGIC     try:
# MAGIC         archive = py7zr.SevenZipFile(f"/dbfs/{landing_zone_vinculos}/compactado/{file.name}", mode='r')
# MAGIC         archive.extractall(path=f"/dbfs/{landing_zone_vinculos}")
# MAGIC         archive.close()
# MAGIC     except:
# MAGIC         print(f"Não foi possível extrair o arquivo {file.name}")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
