from pyspark.sql.functions import (
    col, to_date, upper, trim, when,
    round, current_timestamp, sum as _sum
)

storage_account = "saetlpipeline100"
container       = "datalake100"
base            = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)

# ── Read all 4 raw sources ────────────────────────────────────────
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

print("✓ Raw data loaded")
print(f"  Sales:     {df_sales.count()} rows")
print(f"  Customers: {df_customers.count()} rows")
print(f"  Products:  {df_products.count()} rows")
print(f"  Support:   {df_support.count()} rows")
print(f"  Marketing: {df_marketing.count()} rows")

# ── Clean sales ───────────────────────────────────────────────────
df_sales_clean = (df_sales
    .filter(col("amount") > 0)
    .filter(col("status") != "Cancelled")
    .filter(col("customer_id").isNotNull())
    .withColumn("date",           to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("category",       upper(trim(col("category"))))
    .withColumn("status",         upper(trim(col("status"))))
    .withColumn("region",         upper(trim(col("region"))))
    .withColumn("channel",        upper(trim(col("channel"))))
    .withColumn("payment_method", upper(trim(col("payment_method"))))
)

# ── Clean customers ───────────────────────────────────────────────
df_customers_clean = (df_customers
    .filter(col("customer_id").isNotNull())
    .filter(col("active") == "Yes")
    .withColumn("segment",  upper(trim(col("segment"))))
    .withColumn("country",  trim(col("country")))
    .withColumn("industry", upper(trim(col("industry"))))
    .drop("customer_name")
)

# ── Clean products ────────────────────────────────────────────────
df_products_clean = (df_products
    .filter(col("active") == "Yes")
    .withColumn("margin_pct",
        round((col("unit_price") - col("cost_price")) / col("unit_price") * 100, 2))
    .drop("category")
)

# ── Clean support ─────────────────────────────────────────────────
df_support_clean = (df_support
    .filter(col("customer_id").isNotNull())
    .withColumn("created_date",  to_date(col("created_date"),  "yyyy-MM-dd"))
    .withColumn("resolved_date", to_date(col("resolved_date"), "yyyy-MM-dd"))
    .withColumn("severity",      upper(trim(col("severity"))))
    .withColumn("status",        upper(trim(col("status"))))
    .withColumn("channel",       upper(trim(col("channel"))))
)

# ── Clean marketing ───────────────────────────────────────────────
df_marketing_clean = (df_marketing
    .filter(col("budget_usd") > 0)
    .withColumn("start_date",     to_date(col("start_date"), "yyyy-MM-dd"))
    .withColumn("end_date",       to_date(col("end_date"),   "yyyy-MM-dd"))
    .withColumn("campaign_type",  upper(trim(col("campaign_type"))))
    .withColumn("target_segment", upper(trim(col("target_segment"))))
    .withColumn("target_region",  upper(trim(col("target_region"))))
    .withColumn("roi_pct",
        round((col("spent_usd") / col("budget_usd") - 1) * 100, 2))
)

print("\n✓ Cleaning done")
print(f"  Sales clean:     {df_sales_clean.count()} rows")
print(f"  Customers clean: {df_customers_clean.count()} rows")
print(f"  Products clean:  {df_products_clean.count()} rows")
print(f"  Support clean:   {df_support_clean.count()} rows")
print(f"  Marketing clean: {df_marketing_clean.count()} rows")

# ── Drop overlapping columns from customers before join ───────────
df_customers_join = (df_customers_clean
    .drop("region", "country")
)

# ── Rename overlapping columns from products before join ──────────
df_products_join = (df_products_clean
    .withColumnRenamed("unit_price", "product_unit_price")
)

# ── Join transactions → customers + products ──────────────────────
df_joined = (df_sales_clean
    .join(df_customers_join, on="customer_id", how="left")
    .join(df_products_join,
          df_sales_clean["product"] == df_products_join["product_name"],
          how="left")
    .drop("product_name", "product_id")
)

# ── Calculate total spend per customer ────────────────────────────
customer_spend = (df_joined
    .groupBy("customer_id")
    .agg(_sum("amount").alias("total_spend"))
)

# ── Enrich: customer value segment ───────────────────────────────
df_final = (df_joined
    .join(customer_spend, on="customer_id", how="left")
    .withColumn("customer_segment",
        when(col("total_spend") >= 50000, "High Value")
        .when(col("total_spend") >= 10000, "Mid Value")
        .otherwise("Low Value"))
    .withColumn("pipeline_timestamp", current_timestamp())
    .select(
        "transaction_id", "date", "customer_id", "customer_name",
        "segment", "country", "region", "product", "category",
        "quantity", "unit_price", "discount_pct", "amount",
        "currency", "payment_method", "channel", "shipping_method",
        "status", "customer_segment", "total_spend", "pipeline_timestamp"
    )
)

print(f"\n✓ Final transactions: {df_final.count()} rows")

# ── Write all 5 Delta tables to curated zone ──────────────────────
(df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/transactions/"))
print("✓ transactions written")

(df_customers_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/customers/"))
print("✓ customers written")

(df_products_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/products/"))
print("✓ products written")

(df_support_clean.write
    .format("delta").mode("overwrite")
    .save(f"{base}/curated/support/"))
print("✓ support written")

(df_marketing_clean.write
    .format("delta").mode("overwrite")
    .save(f"{base}/curated/marketing/"))
print("✓ marketing written")

print("\n✓ ALL Delta tables written to curated zone!")
