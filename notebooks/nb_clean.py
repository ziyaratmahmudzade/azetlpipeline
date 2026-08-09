from pyspark.sql.functions import (
    col, to_date, upper, trim, round, current_timestamp
)

storage_account = "saetlpipeline100"
container       = "datalake100"
base            = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)

# ── Read all 4 raw sources (Bronze) ──────────────────────────────
df_sales = (spark.read
    .option("header", True).option("inferSchema", True)
    .csv(f"{base}/raw/sales/sales_transactions (1).csv"))

df_customers = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True).option("inferSchema", True)
    .option("dataAddress", "'Customers'!A1")
    .load(f"{base}/raw/master/master_data (1).xlsx"))

df_products = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True).option("inferSchema", True)
    .option("dataAddress", "'Products'!A1")
    .load(f"{base}/raw/master/master_data (1).xlsx"))

df_support = (spark.read
    .option("multiline", True)
    .json(f"{base}/raw/support/support_tickets.json"))

df_marketing = (spark.read
    .option("header", True).option("inferSchema", True)
    .csv(f"{base}/raw/marketing/marketing_campaigns.csv"))

print("✓ Bronze zone loaded")
print(f"  Sales:     {df_sales.count()} rows")
print(f"  Customers: {df_customers.count()} rows")
print(f"  Products:  {df_products.count()} rows")
print(f"  Support:   {df_support.count()} rows")
print(f"  Marketing: {df_marketing.count()} rows")

# ── Clean and validate — Silver layer ────────────────────────────

# Silver: sales
df_sales_silver = (df_sales
    .filter(col("amount") > 0)
    .filter(col("status") != "Cancelled")
    .filter(col("customer_id").isNotNull())
    .withColumn("date",           to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("category",       upper(trim(col("category"))))
    .withColumn("status",         upper(trim(col("status"))))
    .withColumn("region",         upper(trim(col("region"))))
    .withColumn("channel",        upper(trim(col("channel"))))
    .withColumn("payment_method", upper(trim(col("payment_method"))))
    .withColumn("ingested_at",    current_timestamp())
)

# Silver: customers
df_customers_silver = (df_customers
    .filter(col("customer_id").isNotNull())
    .filter(col("active") == "Yes")
    .withColumn("segment",     upper(trim(col("segment"))))
    .withColumn("country",     trim(col("country")))
    .withColumn("industry",    upper(trim(col("industry"))))
    .withColumn("ingested_at", current_timestamp())
    .drop("customer_name")
)

# Silver: products
df_products_silver = (df_products
    .filter(col("active") == "Yes")
    .withColumn("margin_pct",
        round((col("unit_price") - col("cost_price")) / col("unit_price") * 100, 2))
    .withColumn("ingested_at", current_timestamp())
    .drop("category")
)

# Silver: support
df_support_silver = (df_support
    .filter(col("customer_id").isNotNull())
    .withColumn("created_date",  to_date(col("created_date"),  "yyyy-MM-dd"))
    .withColumn("resolved_date", to_date(col("resolved_date"), "yyyy-MM-dd"))
    .withColumn("severity",      upper(trim(col("severity"))))
    .withColumn("status",        upper(trim(col("status"))))
    .withColumn("channel",       upper(trim(col("channel"))))
    .withColumn("ingested_at",   current_timestamp())
)

# Silver: marketing
df_marketing_silver = (df_marketing
    .filter(col("budget_usd") > 0)
    .withColumn("start_date",     to_date(col("start_date"), "yyyy-MM-dd"))
    .withColumn("end_date",       to_date(col("end_date"),   "yyyy-MM-dd"))
    .withColumn("campaign_type",  upper(trim(col("campaign_type"))))
    .withColumn("target_segment", upper(trim(col("target_segment"))))
    .withColumn("target_region",  upper(trim(col("target_region"))))
    .withColumn("roi_pct",
        round((col("spent_usd") / col("budget_usd") - 1) * 100, 2))
    .withColumn("ingested_at",    current_timestamp())
)

print("\n✓ Silver layer cleaning done")
print(f"  Sales silver:     {df_sales_silver.count()} rows")
print(f"  Customers silver: {df_customers_silver.count()} rows")
print(f"  Products silver:  {df_products_silver.count()} rows")
print(f"  Support silver:   {df_support_silver.count()} rows")
print(f"  Marketing silver: {df_marketing_silver.count()} rows")

# ── Write Silver Delta tables to clean zone ───────────────────────
(df_sales_silver.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/cleaned/transactions/"))
print("✓ transactions → cleaned written")

(df_customers_silver.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/cleaned/customers/"))
print("✓ customers → cleaned written")

(df_products_silver.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/cleaned/products/"))
print("✓ products → cleaned written")

(df_support_silver.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/cleaned/support/"))
print("✓ support → cleaned written")

(df_marketing_silver.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/cleaned/marketing/"))
print("✓ marketing → cleaned written")

print("\n✓ ALL Silver Delta tables written to cleaned zone!")