# Databricks notebook source
# DBTITLE 1,Silver transformation for ventas
import pyspark.sql.functions as F

# 1. Leer de Bronze
df_bronze = spark.table("bronze_ventas_tco")

# 2. Limpieza y casteo de tipos usando tus columnas reales
df_silver = (
    df_bronze
    .filter(F.col("C_digo_producto").isNotNull())
    .filter(~F.col("C_digo_producto").isin("Total General", "Procesado en: septiembre 11 2026 17:13"))
    .withColumn("Cantidad_Vendida", F.col("Cantidad_vendida").cast("double"))
    .withColumn("Total_Num", F.col("Total").cast("double"))
    .withColumn("Valor_Bruto_Num", F.col("Valor_bruto").cast("double"))
    # Enriquecer con fecha de ingesta
    .withColumn("Fecha_Procesamiento", F.current_timestamp())
)

# 3. Guardar en Silver
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_ventas_tco")

# COMMAND ----------

