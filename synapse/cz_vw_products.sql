use etl_db;
GO

CREATE OR ALTER VIEW vw_products AS
SELECT
    CAST(product_name AS VARCHAR(200))      AS product_name,
    CAST(subcategory AS VARCHAR(100))       AS subcategory,
    CAST(unit_price AS FLOAT)               AS unit_price,
    CAST(cost_price AS FLOAT)               AS cost_price,
    CAST(margin_pct AS FLOAT)               AS margin_pct,
    CAST(supplier AS VARCHAR(200))          AS supplier,
    CAST(stock_qty AS INT)                  AS stock_qty,
    CAST(reorder_level AS INT)              AS reorder_level,
    CAST(lead_time_days AS INT)             AS lead_time_days,
    CAST(warranty_months AS INT)            AS warranty_months,
    CAST(weight_kg AS FLOAT)               AS weight_kg,
    CAST(sku AS VARCHAR(50))               AS sku,
    CAST(active AS VARCHAR(5))             AS active,
    CAST(margin_pct AS FLOAT)              AS margin_calculated
FROM OPENROWSET(
    BULK 'products/',
    DATA_SOURCE = 'curated_zone',
    FORMAT = 'DELTA'
) AS [result];