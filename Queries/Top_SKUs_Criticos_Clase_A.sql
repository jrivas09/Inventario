-- Top SKUs Críticos - Clase A
-- Identifica los productos Clase A (80% del ingreso acumulado)
SELECT 
    C_digo_producto,
    Nombre_producto,     
    ROUND(Venta_Total_SKU, 0) AS Ingreso_COP, 
    ROUND(Pct_Acumulado * 100, 2) AS Porcentaje_Acumulado
FROM gold_ventas_tco
WHERE Clase_ABC = 'A'
ORDER BY Venta_Total_SKU DESC;