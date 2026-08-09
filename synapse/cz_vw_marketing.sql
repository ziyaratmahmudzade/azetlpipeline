use etl_db;
GO

CREATE OR ALTER VIEW vw_marketing AS
SELECT
    CAST(campaign_id AS VARCHAR(20))        AS campaign_id,
    CAST(campaign_name AS VARCHAR(500))     AS campaign_name,
    CAST(campaign_type AS VARCHAR(100))     AS campaign_type,
    CAST(objective AS VARCHAR(100))         AS objective,
    CAST(platform AS VARCHAR(100))          AS platform,
    CAST(target_segment AS VARCHAR(50))     AS target_segment,
    CAST(target_region AS VARCHAR(50))      AS target_region,
    CAST(start_date AS DATE)                AS start_date,
    CAST(end_date AS DATE)                  AS end_date,
    CAST(duration_days AS INT)              AS duration_days,
    CAST(budget_usd AS FLOAT)               AS budget_usd,
    CAST(spent_usd AS FLOAT)                AS spent_usd,
    CAST(impressions AS BIGINT)             AS impressions,
    CAST(clicks AS INT)                     AS clicks,
    CAST(ctr_pct AS FLOAT)                  AS ctr_pct,
    CAST(leads_generated AS INT)            AS leads_generated,
    CAST(conversions AS INT)                AS conversions,
    CAST(roi_pct AS FLOAT)                  AS roi_pct
FROM OPENROWSET(
    BULK 'marketing/',
    DATA_SOURCE = 'curated_zone',
    FORMAT = 'DELTA'
) AS [result];