# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Set schema and load data

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,current_timestamp,lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

# COMMAND ----------

rais_schema=StructType(fields=[StructField("Bairros SP",StringType(),False),
                                  StructField("Bairros Fortaleza",StringType(),False),
                                  StructField("Bairros RJ",StringType(),False),
                                  StructField("Causa Afastamento 1",StringType(),False),
                                  StructField("Causa Afastamento 2",StringType(),True),
                                  StructField("Causa Afastamento 3",StringType(),False),
                                  StructField("Motivo Desligamento",StringType(),True),
                                  StructField("CBO Ocupação 2002",StringType(),False),
                                  StructField("CNAE 2.0 Classe",StringType(),False),
                                  StructField("CNAE 95 Classe",StringType(),False),
                                  StructField("Distritos SP",StringType(),True),
                                  StructField("Vínculo Ativo 31/12",StringType(),True),
                                  StructField("Faixa Etária",StringType(),True),
                                  StructField("Faixa Hora Contrat",StringType(),True),
                                  StructField("Faixa Remun Dezem (SM)",StringType(),True),
                                  StructField("Faixa Remun Média (SM)",StringType(),True),
                                  StructField("Faixa Tempo Emprego",StringType(),True),
                                  StructField("Escolaridade após 2005",StringType(),True),
                                  StructField("Qtd Hora Contr",IntegerType(),True),
                                  StructField("Idade",IntegerType(),True),
                                  StructField("Ind CEI Vinculado",StringType(),True),
                                  StructField("Ind Simples",StringType(),True),
                                  StructField("Mês Admissão",IntegerType(),True),
                                  StructField("Mês Desligamento",IntegerType(),True),
                                  StructField("Mun Trab",StringType(),True),
                                  StructField("Município",StringType(),True),
                                  StructField("Nacionalidade",StringType(),False),
                                  StructField("Natureza Jurídica",StringType(),True),
                                  StructField("Ind Portador Defic",StringType(),True),
                                  StructField("Qtd Dias Afastamento",IntegerType(),True),
                                  StructField("Raça Cor",StringType(),True),
                                  StructField("Regiões Adm DF",StringType(),True),
                                  StructField("Vl Remun Dezembro Nom",DoubleType(),True),
                                  StructField("Vl Remun Dezembro (SM)",DoubleType(),True),
                                  StructField("Vl Remun Média Nom",DoubleType(),True),
                                  StructField("Vl Remun Média (SM)",DoubleType(),True),
                                  StructField("CNAE 2.0 Subclasse",StringType(),True),
                                  StructField("Sexo Trabalhador",StringType(),True),
                                  StructField("Tamanho Estabelecimento",StringType(),True),
                                  StructField("Tempo Emprego",DoubleType(),True),
                                  StructField("Tipo Admissão",StringType(),True),
                                  StructField("Tipo Estab41",StringType(),True),
                                  StructField("Tipo Estab42",StringType(),True),
                                  StructField("Tipo Defic",StringType(),True),
                                  StructField("Tipo Vínculo",StringType(),True),
                                  StructField("IBGE Subsetor",StringType(),True),
                                  StructField("Vl Rem Janeiro SC",DoubleType(),True),
                                  StructField("Vl Rem Fevereiro SC",DoubleType(),True),
                                  StructField("Vl Rem Março SC",DoubleType(),True),
                                  StructField("Vl Rem Abril SC",DoubleType(),True),
                                  StructField("Vl Rem Maio SC",DoubleType(),True),
                                  StructField("Vl Rem Junho SC",DoubleType(),True),
                                  StructField("Vl Rem Julho SC",DoubleType(),True),
                                  StructField("Vl Rem Agosto SC",DoubleType(),True),
                                  StructField("Vl Rem Setembro SC",DoubleType(),True),
                                  StructField("Vl Rem Outubro SC",DoubleType(),True),
                                  StructField("Vl Rem Novembro SC",DoubleType(),True),
                                  StructField("Ano Chegada Brasil",IntegerType(),True),
                                  StructField("Ind Trab Intermitente",StringType(),True),
                                  StructField("Ind Trab Parcial",StringType(),True)])

# COMMAND ----------

df_vinculos=spark.read.format("csv").options(header=True,delimiter=";",encoding='ISO-8859-1').load("/Volumes/raw/rais/vinculos/*.txt")
df_vinculos.display()

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

# def cast_string_to_integer(columns,df):
#     for column in columns:
#         df=df.withColumn(column,df[column].cast('integer'))
#     return df
    


# COMMAND ----------

# integer_format_columns=["Qtd_Hora_Contr", "Idade", "Qtd_Dias_Afastamento","Ano_Chegada_Brasil"]
# df_vinculos_bronze=cast_string_to_integer(integer_format_columns, df_vinculos_bronze)

# COMMAND ----------

# def cast_string_to_double(columns_list, df):
#     for column in columns_list:
#         df = df.withColumn(column, regexp_replace(column, ',', '.'))
#         df=df.withColumn(column,df[column].cast(DoubleType()))
#     return df


# COMMAND ----------

# double_format_columns=["Tempo_Emprego","Vl_Remun_Dezembro_Nom","Vl_Remun_Dezembro_SM","Vl_Remun_Média_Nom","Vl_Remun_Média_SM","Vl_Rem_Janeiro_SC","Vl_Rem_Fevereiro_SC","Vl_Rem_Março_SC","Vl_Rem_Abril_SC","Vl_Rem_Maio_SC","Vl_Rem_Junho_SC","Vl_Rem_Julho_SC","Vl_Rem_Agosto_SC","Vl_Rem_Setembro_SC","Vl_Rem_Outubro_SC","Vl_Rem_Novembro_SC"]
# df_vinculos_bronze=cast_string_to_double(double_format_columns,df_vinculos_bronze)

# COMMAND ----------

df_vinculos_bronze \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.option('path', 'abfss://rais@raispipeline.dfs.core.windows.net/2021/bronze/vinculos/') \
.saveAsTable('bronze.rais.vinculos')
