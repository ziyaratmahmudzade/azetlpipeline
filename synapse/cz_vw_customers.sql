use etl_db;
GO

CREATE OR ALTER VIEW vw_customers AS
SELECT
    CAST(customer_id AS VARCHAR(20))        AS customer_id,
    CAST(segment AS VARCHAR(50))            AS segment,
    CAST(country AS VARCHAR(100))           AS country,
    CAST(city AS VARCHAR(100))              AS city,
    CAST(region AS VARCHAR(50))             AS region,
    CAST(email AS VARCHAR(200))             AS email,
    CAST(phone AS VARCHAR(50))              AS phone,
    CAST(account_manager AS VARCHAR(200))   AS account_manager,
    CAST(credit_limit AS FLOAT)             AS credit_limit,
    CAST(since AS VARCHAR(20))              AS since,
    CAST(annual_revenue_usd AS FLOAT)       AS annual_revenue_usd,
    CAST(employee_count AS INT)             AS employee_count,
    CAST(industry AS VARCHAR(100))          AS industry,
    CAST(preferred_currency AS VARCHAR(10)) AS preferred_currency,
    CAST(payment_terms AS VARCHAR(50))      AS payment_terms,
    CAST(active AS VARCHAR(5))              AS active
FROM OPENROWSET(
    BULK 'customers/',
    DATA_SOURCE = 'curated_zone',
    FORMAT = 'DELTA'
) AS [result];