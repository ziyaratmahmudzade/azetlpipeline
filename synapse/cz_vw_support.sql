use etl_db;
GO

CREATE OR ALTER VIEW vw_support AS
SELECT
    CAST(ticket_id AS VARCHAR(20))          AS ticket_id,
    CAST(customer_id AS VARCHAR(20))        AS customer_id,
    CAST(customer_name AS VARCHAR(200))     AS customer_name,
    CAST(segment AS VARCHAR(50))            AS segment,
    CAST(region AS VARCHAR(50))             AS region,
    CAST(ticket_type AS VARCHAR(100))       AS ticket_type,
    CAST(severity AS VARCHAR(50))           AS severity,
    CAST(status AS VARCHAR(50))             AS status,
    CAST(subject AS VARCHAR(500))           AS subject,
    CAST(created_date AS DATE)              AS created_date,
    CAST(resolved_date AS DATE)             AS resolved_date,
    CAST(resolution_days AS INT)            AS resolution_days,
    CAST(assigned_agent AS VARCHAR(200))    AS assigned_agent,
    CAST(satisfaction_score AS INT)         AS satisfaction_score,
    CAST(channel AS VARCHAR(50))            AS channel,
    CAST(related_product_id AS VARCHAR(20)) AS related_product_id
FROM OPENROWSET(
    BULK 'support/',
    DATA_SOURCE = 'curated_zone',
    FORMAT = 'DELTA'
) AS [result];