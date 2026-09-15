# Databricks notebook source
# DBTITLE 1,Silver transformation for compras
import pyspark.sql.functions as F

# 1. Leer de Bronze
df_bronze = spark.table("bronze_compras_tco")

# 2. Limpieza y casteo de tipos usando las columnas reales
df_silver = (
    df_bronze
    .filter(F.col("C_digo_producto").isNotNull())
    .filter(~F.col("C_digo_producto").isin("Total General"))
    .filter(~F.col("C_digo_producto").startswith("Procesado en:"))
    .withColumn("Cantidad_Comprada", F.col("Cantidad").cast("double"))
    .withColumn("Total_Num", F.col("Total").cast("double"))
    .withColumn("Valor_Bruto_Num", F.col("Valor_bruto").cast("double"))
    # Enriquecer con fecha de ingesta
    .withColumn("Fecha_Procesamiento", F.current_timestamp())
)

# 3. Guardar en Silver
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_compras_tco")

# COMMAND ----------



# COMMAND ----------

