from pyspark.sql.functions import (
    col, when, current_timestamp, sum as _sum, round
)

storage_account = "saetlpipeline100"
container       = "datalake100"
base            = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)

# ── Read Silver Delta tables ──────────────────────────────────────
df_sales     = spark.read.format("delta").load(f"{base}/cleaned/transactions/")
df_customers = spark.read.format("delta").load(f"{base}/cleaned/customers/")
df_products  = spark.read.format("delta").load(f"{base}/cleaned/products/")
df_support   = spark.read.format("delta").load(f"{base}/cleaned/support/")
df_marketing = spark.read.format("delta").load(f"{base}/cleaned/marketing/")

print("✓ Silver zone loaded")
print(f"  Sales:     {df_sales.count()} rows")
print(f"  Customers: {df_customers.count()} rows")
print(f"  Products:  {df_products.count()} rows")
print(f"  Support:   {df_support.count()} rows")
print(f"  Marketing: {df_marketing.count()} rows")

# ── Drop overlapping columns before join ──────────────────────────
df_customers_join = df_customers.drop("region", "country")
df_products_join  = df_products.withColumnRenamed("unit_price", "product_unit_price")

# ── Join transactions → customers + products ──────────────────────
df_joined = (df_sales
    .join(df_customers_join, on="customer_id", how="left")
    .join(df_products_join,
          df_sales["product"] == df_products_join["product_name"],
          how="left")
    .drop("product_name", "product_id")
)

# ── Calculate total spend per customer ────────────────────────────
customer_spend = (df_joined
    .groupBy("customer_id")
    .agg(_sum("amount").alias("total_spend"))
)

# ── Enrich: customer value segment ───────────────────────────────
df_transactions_gold = (df_joined
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

print(f"\n✓ Gold enrichment done")
print(f"  Transactions gold: {df_transactions_gold.count()} rows")

# ── Write Gold Delta tables to curated zone ───────────────────────
(df_transactions_gold.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/transactions/"))
print("✓ transactions → gold written")

(df_customers.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/customers/"))
print("✓ customers → gold written")

(df_products.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/products/"))
print("✓ products → gold written")

(df_support.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/support/"))
print("✓ support → gold written")

(df_marketing.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{base}/curated/marketing/"))
print("✓ marketing → gold written")

print("\n✓ ALL Gold Delta tables written to curated zone!")