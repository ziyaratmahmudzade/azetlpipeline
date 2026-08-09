USE etl_db;
GO
 
SELECT 'transactions' AS table_name, COUNT(*) AS row_count FROM vw_transactions
UNION ALL
SELECT 'customers',  COUNT(*) FROM vw_customers
UNION ALL
SELECT 'products',   COUNT(*) FROM vw_products
UNION ALL
SELECT 'support',    COUNT(*) FROM vw_support
UNION ALL
SELECT 'marketing',  COUNT(*) FROM vw_marketing;