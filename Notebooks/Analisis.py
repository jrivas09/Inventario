# Databricks notebook source
# DBTITLE 1,Análisis ABC del Portafolio TCO SAS
import pyspark.sql.functions as F

# Análisis Descriptivo del Portafolio TCO SAS
resumen_abc = (
    spark.table("gold_ventas_tco")
    .groupBy("Clase_ABC")
    .agg(
        F.count("C_digo_producto").alias("Cantidad_SKUs"),
        F.sum("Venta_Total_SKU").alias("Ingreso_Generado_COP"),
        F.round((F.count("C_digo_producto") / F.countDistinct("C_digo_producto")) * 100, 2).alias("Pct_del_Portafolio")
    )
    .orderBy("Clase_ABC")
)
display(resumen_abc)

# COMMAND ----------

