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

# ── Read raw data ─────────────────────────────────────────────────
df_sales = (spark.read
    .option("header", True).option("inferSchema", True)
    .csv(f"{base}/raw/sales/sales_transactions.csv"))

df_customers = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True).option("inferSchema", True)
    .option("dataAddress", "'Customers'!A1")
    .load(f"{base}/raw/master/master_data.xlsx"))

df_products = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True).option("inferSchema", True)
    .option("dataAddress", "'Products'!A1")
    .load(f"{base}/raw/master/master_data.xlsx"))

print("✓ Raw data loaded")
print(f"  Sales:     {df_sales.count()} rows")
print(f"  Customers: {df_customers.count()} rows")
print(f"  Products:  {df_products.count()} rows")

# ── Clean sales ───────────────────────────────────────────────────
df_sales_clean = (df_sales
    .filter(col("amount") > 0)
    .filter(col("status") != "Cancelled")
    .filter(col("customer_id").isNotNull())
    .withColumn("date",     to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("status",   upper(trim(col("status"))))
    .withColumn("region",   upper(trim(col("region"))))
)

# ── Clean customers ───────────────────────────────────────────────
df_customers_clean = (df_customers
    .filter(col("customer_id").isNotNull())
    .filter(col("customer_name") != "Unknown Corp")
    .withColumn("segment", upper(trim(col("segment"))))
    .withColumn("country", trim(col("country")))
    .drop("customer_name")        # drop duplicate — sales has this already
)

# ── Clean products ────────────────────────────────────────────────
df_products_clean = (df_products
    .withColumn("margin",
        round((col("unit_price") - col("cost_price")) / col("unit_price") * 100, 2))
    .drop("category")             # drop duplicate — sales has this already
)

print("✓ Cleaning done")
print(f"  Sales after cleaning:     {df_sales_clean.count()} rows")
print(f"  Customers after cleaning: {df_customers_clean.count()} rows")
print(f"  Products columns: {df_products_clean.columns}")

# ── Rename overlapping product columns before join ────────────────
df_products_join = (df_products_clean
    .withColumnRenamed("unit_price", "product_unit_price")
    .drop("category")
)

# ── Join transactions → customers + products ──────────────────────
df_joined = (df_sales_clean
    .join(df_customers_clean, on="customer_id", how="left")
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

# ── Enrich: add customer value segment ───────────────────────────
df_enriched = (df_joined
    .join(customer_spend, on="customer_id", how="left")
    .withColumn("customer_segment",
        when(col("total_spend") >= 5000, "High Value")
        .when(col("total_spend") >= 1000, "Mid Value")
        .otherwise("Low Value"))
    .withColumn("pipeline_timestamp", current_timestamp())
)

# ── Select final columns ──────────────────────────────────────────
df_final = df_enriched.select(
    "transaction_id", "date", "customer_id", "customer_name",
    "segment", "country", "region", "product", "category",
    "quantity", "unit_price", "amount", "margin",
    "status", "customer_segment", "total_spend", "pipeline_timestamp"
)

print(f"✓ Enrichment done — final rows: {df_final.count()}")
display(df_final)

# ── Write transactions to curated zone ───────────────────────────
(df_final.write
    .format("delta")
    .mode("overwrite")
    .save(f"{base}/curated/transactions/"))

print("✓ transactions written")

# ── Write customers to curated zone ──────────────────────────────
(df_customers_clean.write
    .format("delta")
    .mode("overwrite")
    .save(f"{base}/curated/customers/"))

print("✓ customers written")

# ── Write products to curated zone ───────────────────────────────
(df_products_clean.write
    .format("delta")
    .mode("overwrite")
    .save(f"{base}/curated/products/"))

print("✓ products written")
print("\n✓ ALL Delta tables written to curated zone")

print("=== curated/transactions/ ===")
for f in dbutils.fs.ls(f"{base}/curated/transactions/"):
    print(f.name)

print("\n=== curated/customers/ ===")
for f in dbutils.fs.ls(f"{base}/curated/customers/"):
    print(f.name)

print("\n=== curated/products/ ===")
for f in dbutils.fs.ls(f"{base}/curated/products/"):
    print(f.name)
