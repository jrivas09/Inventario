# Databricks notebook source
# DBTITLE 1,Gold transformation with ABC analysis for compras
import pyspark.sql.functions as F
from pyspark.sql.window import Window

df_silver = spark.table("silver_compras_tco")

# 1. Agrupar el total de compras por SKU y Nombre
df_agg = df_silver.groupBy("C_digo_producto", "Nombre_producto").agg(
    F.sum("Total_Num").alias("Compra_Total_SKU"),
    F.sum("Cantidad_Comprada").alias("Volumen_Total_SKU")
)

# 2. Calcular la Compra Global
compra_global = df_agg.select(F.sum("Compra_Total_SKU")).collect()[0][0]

# 3. Calcular % Acumulado y Clasificación Pareto
window_spec = Window.orderBy(F.col("Compra_Total_SKU").desc())

df_gold = (
    df_agg
    .withColumn("Pct_Compra", F.col("Compra_Total_SKU") / compra_global)
    .withColumn("Pct_Acumulado", F.sum("Pct_Compra").over(window_spec))
    .withColumn(
        "Clase_ABC",
        F.when(F.col("Pct_Acumulado") <= 0.80, "A")
         .when(F.col("Pct_Acumulado") <= 0.95, "B")
         .otherwise("C")
    )
)

# 4. Guardar como tabla final
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_compras_tco")

# COMMAND ----------



# COMMAND ----------

