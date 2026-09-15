-- Distribución Pareto
-- Cantidad de SKUs por cada clase ABC
SELECT Clase_ABC, COUNT(C_digo_producto) AS Cantidad_SKUs
FROM gold_ventas_tco
GROUP BY Clase_ABC
ORDER BY Clase_ABC;