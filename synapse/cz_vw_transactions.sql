use etl_db;
GO

CREATE OR ALTER VIEW vw_transactions AS
SELECT
    CAST(transaction_id AS VARCHAR(20))  AS transaction_id,
    CAST(date AS DATE)                   AS date,
    CAST(customer_id AS VARCHAR(20))     AS customer_id,
    CAST(customer_name AS VARCHAR(200))  AS customer_name,
    CAST(segment AS VARCHAR(50))         AS segment,
    CAST(country AS VARCHAR(100))        AS country,
    CAST(region AS VARCHAR(50))          AS region,
    CAST(product AS VARCHAR(200))        AS product,
    CAST(category AS VARCHAR(100))       AS category,
    CAST(quantity AS INT)                AS quantity,
    CAST(unit_price AS FLOAT)            AS unit_price,
    CAST(discount_pct AS FLOAT)          AS discount_pct,
    CAST(amount AS FLOAT)                AS amount,
    CAST(currency AS VARCHAR(10))        AS currency,
    CAST(payment_method AS VARCHAR(50))  AS payment_method,
    CAST(channel AS VARCHAR(50))         AS channel,
    CAST(shipping_method AS VARCHAR(50)) AS shipping_method,
    CAST(status AS VARCHAR(50))          AS status,
    CAST(customer_segment AS VARCHAR(50))AS customer_segment,
    CAST(total_spend AS FLOAT)           AS total_spend,
    CAST(pipeline_timestamp AS VARCHAR(50)) AS pipeline_timestamp
FROM OPENROWSET(
    BULK 'transactions/',
    DATA_SOURCE = 'curated_zone',
    FORMAT = 'DELTA'
) AS [result];