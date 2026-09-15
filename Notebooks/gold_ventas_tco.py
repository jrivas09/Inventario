# Databricks notebook source
# DBTITLE 1,Gold transformation with ABC analysis for ventas
import pyspark.sql.functions as F
from pyspark.sql.window import Window

df_silver = spark.table("silver_ventas_tco")

# 1. Agrupar el total de ventas por SKU y Nombre
df_agg = df_silver.groupBy("C_digo_producto", "Nombre_producto").agg(
    F.sum("Total").alias("Venta_Total_SKU"),
    F.sum("Cantidad_Vendida").alias("Volumen_Total_SKU")
)

# 2. Calcular la Venta Global
venta_global = df_agg.select(F.sum("Venta_Total_SKU")).collect()[0][0]

# 3. Calcular % Acumulado y Clasificación Pareto
window_spec = Window.orderBy(F.col("Venta_Total_SKU").desc())

df_gold = (
    df_agg
    .withColumn("Pct_Venta", F.col("Venta_Total_SKU") / venta_global)
    .withColumn("Pct_Acumulado", F.sum("Pct_Venta").over(window_spec))
    .withColumn(
        "Clase_ABC",
        F.when(F.col("Pct_Acumulado") <= 0.80, "A")
         .when(F.col("Pct_Acumulado") <= 0.95, "B")
         .otherwise("C")
    )
)

# 4. Guardar como tabla final
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_ventas_tco")

# COMMAND ----------

