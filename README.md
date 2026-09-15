# INVENTARIO REPUESTOS TCO 
 

1. Caso de Negocio  

Descripción del problema: La COMERCIALIZADORA DE REPUESTOS TCO SAS maneja 514 códigos de productos y servicios. Actualmente, se invierte el mismo esfuerzo operativo y de almacenamiento en 428 SKUs de baja rotación (Clase C) que en los 6 SKUs o servicios estrella (Clase A) que mantienen la rentabilidad de la empresa. Procesar manualmente archivos como "Ventas por producto.xlsx" y "Compras por producto.xlsx" con miles de registros genera cuellos de botella e impide una toma de decisiones ágil.  

Objetivo del proyecto: Desarrollar un pipeline automatizado de Big Data en Databricks que ingeste el historial de ventas y compras, para segmentar el portafolio de productos dinámicamente bajo el modelo ABC, optimizando la gestión del inventario y focalizando los esfuerzos comerciales en el 1% de los productos que generan el 80% del valor. 

2. Análisis Económico y Relación Beneficio/Coste  

Análisis Económico (Ahorro en horas-hombre): Automatizar la conciliación de compras (52.7M) y ventas (3.46B) elimina el trabajo manual en Excel. Si un especialista de inventarios (costo de ~$25.000 COP/hora) ahorra 40 horas mensuales, se justifican $1.000.000 COP mensuales en ahorros operativos puros.  

Retorno de Inversión (ROI): 

Costos: Implementar la arquitectura nube (Databricks) cuesta aproximadamente $150 USD/mes (~$600.000 COP).  

Mayores Ingresos: Reducir en un 2% los quiebres de stock en los 6 productos de la Clase A aumentaría los ingresos brutos anuales proyectados. 

Conclusión: El ahorro operativo por sí solo supera el costo de la nube, garantizando un ROI positivo desde el mes 1, sumado al beneficio de mayores ventas.  

3. Diagrama de Arquitectura  

Puedes dibujar el siguiente flujo en tu herramienta de diagramado favorita y subir la imagen de alta calidad a la carpeta /architecture de tu repositorio:  

Explicación Técnica: Los archivos Excel se ingieren utilizando un entorno de nube gestionado por Databricks. La información fluye secuencialmente enriqueciéndose en cada etapa de la arquitectura Medallion, persistiendo los datos con transacciones ACID de Delta Lake. 

 

4. PIPELINE: Ingesta de datos automatizada

Dado que tus archivos están en formato Excel y tienen 6 filas de encabezados corporativos antes de las columnas reales, primero debemos leerlos con Pandas y convertirlos a Spark. 

Crea un notebook llamado pipeline_tco_sas.py y utiliza este código adaptado a tus columnas exactas: 

4.1. Capa Bronze (Datos Crudos) 

 

import pandas as pd 

import pyspark.sql.functions as F 

from pyspark.sql.window import Window 

 

# 1. Leer los archivos Excel

pdf_ventas = pd.read_excel("/dbfs/FileStore/Ventas por producto.xlsx", skiprows=6) 

pdf_compras = pd.read_excel("/dbfs/FileStore/Compras por producto.xlsx", skiprows=6) 

 

# 2. Limpiar nulos completos (filas vacías al final) 

pdf_ventas.dropna(how='all', inplace=True) 

 

# 3. Convertir a Spark  

df_ventas_raw = spark.createDataFrame(pdf_ventas.astype(str)) 

 

# 4. Guardar en Capa Bronze 

df_ventas_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_ventas_tco") 

 

4.2. Capa Silver (Transformaciones y Enriquecimiento) 

 

# 1. Leer de Bronze 

df_bronze = spark.table("bronze_ventas_tco") 

 

# 2. Limpieza y casteo de tipos usando tus columnas reales 

df_silver = ( 

    df_bronze 

    .filter(F.col("Código producto").isNotNull()) 

    .withColumn("Cantidad_Vendida", F.col("Cantidad vendida").cast("double")) 

    .withColumn("Total", F.col("Total").cast("double")) 

    .withColumn("Valor_Bruto", F.col("Valor bruto").cast("double")) 

    # Enriquecer con fecha de ingesta 

    .withColumn("Fecha_Procesamiento", F.current_timestamp()) 

) 

 

# 3. Guardar en Silver 

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_ventas_tco") 

 

4.3. Capa Gold (Datos listos para el negocio - Modelo ABC) 

 

df_silver = spark.table("silver_ventas_tco") 

 

# 1. Agrupar el total de ventas por SKU y Nombre 

df_agg = df_silver.groupBy("Código producto", "Nombre producto").agg( 

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

df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_ventas_abc") 

 

 

 

5. Modelado y Análisis  

Para cumplir con el análisis descriptivo, ejecuta este código que evidencia la realidad de tu negocio basándose en tus Excels. Puedes acompañarlo registrando tiempos de ejecución o distribuciones en MLflow si deseas mayor rigor científico: 

# Análisis Descriptivo del Portafolio TCO SAS 

resumen_abc = ( 

    spark.table("gold_ventas_abc") 

    .groupBy("Clase_ABC") 

    .agg( 

        F.count("Código producto").alias("Cantidad_SKUs"), 

        F.sum("Venta_Total_SKU").alias("Ingreso_Generado_COP"), 

        F.round((F.count("Código producto") / 514) * 100, 2).alias("Pct_del_Portafolio") 

    ) 

    .orderBy("Clase_ABC") 

) 

display(resumen_abc) 

 

6. APP o Visualización y Serving Endpoint

Serving Endpoint: Configura tu Databricks SQL Warehouse para exponer la tabla gold_ventas_abc. Deberás mostrar evidencia (capturas de pantalla) de que el endpoint está activo y listo para ser consumido.  

Visualización: Crea un Dashboard en Databricks SQL con imágenes de alta resolución (como sugiere el PDF de evaluación) mostrando:  

Un KPI con el Total de Ventas ($3.460 millones COP). 

Una tabla detallando el "Top 5" de productos Clase A (Ej. Reparación Interna Hidráulico). 

Un gráfico de pastel contrastando que el 83% de tu inventario físico (Clase C) genera una fracción mínima de los ingresos. 

 

 



