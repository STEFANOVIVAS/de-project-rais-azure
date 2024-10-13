# Databricks notebook source
import py7zr

landing_zone = '/mnt/raispipeline/rais/year/2022/landing_zone/'
raw_layer = '/mnt/raispipeline/rais/year/2022/raw_layer/'

for file in dbutils.fs.ls(landing_zone):
        try:
            archive = py7zr.SevenZipFile(f"/dbfs{landing_zone}{file.name}", mode='r')
            archive.extractall(path=f"/dbfs{raw_layer}")
            archive.close()
        except:
            print(f"Não foi possível extrair o arquivo {file.name}")






