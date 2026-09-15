# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Ingesta Bronze - Excel a Delta
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# 1. Leer los archivos Excel saltando las 6 filas iniciales de membrete
pdf_ventas = pd.read_excel("/Workspace/Users/john.rivas1509@unaula.edu.co/Inventario/Ventas por producto.xlsx", skiprows=6)
pdf_compras = pd.read_excel("/Workspace/Users/john.rivas1509@unaula.edu.co/Inventario/Compras por producto.xlsx", skiprows=6)

# 2. Limpiar nulos completos (filas vacías al final)
pdf_ventas.dropna(how='all', inplace=True)
pdf_compras.dropna(how='all', inplace=True)

# 3. Limpiar nombres de columnas (remover espacios y caracteres especiales)
import re
pdf_ventas.columns = [re.sub(r'[^a-zA-Z0-9]', '_', col).strip('_') for col in pdf_ventas.columns]
pdf_compras.columns = [re.sub(r'[^a-zA-Z0-9]', '_', col).strip('_') for col in pdf_compras.columns]

# 4. Convertir a Spark DataFrames
df_ventas_raw = spark.createDataFrame(pdf_ventas.astype(str))
df_compras_raw = spark.createDataFrame(pdf_compras.astype(str))

# 5. Guardar en Capa Bronze
df_ventas_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_ventas_tco")
df_compras_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_compras_tco")

# COMMAND ----------

