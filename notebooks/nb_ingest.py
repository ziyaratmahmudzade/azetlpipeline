# from pyspark.sql.functions import col, to_date, upper, trim, when, round, current_timestamp, sum as _sum, datediff, lit

storage_account = "saetlpipeline100"
container       = "datalake100"
base            = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)

# ── 1. Read sales transactions CSV ────────────────────────────────
df_sales = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base}/raw/sales/sales_transactions (1).csv"))

display(df_sales)

# ── 2. Read master data XLSX ──────────────────────────────────────
df_customers = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True)
    .option("inferSchema", True)
    .option("dataAddress", "'Customers'!A1")
    .load(f"{base}/raw/master/master_data (1).xlsx"))

display(df_customers)

df_products = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True)
    .option("inferSchema", True)
    .option("dataAddress", "'Products'!A1")
    .load(f"{base}/raw/master/master_data (1).xlsx"))

display(df_products)

# ── 3. Read support tickets JSON ──────────────────────────────────
df_support = (spark.read
    .option("multiline", True)
    .json(f"{base}/raw/support/support_tickets.json"))

display(df_support)

# ── 4. Read marketing campaigns CSV ───────────────────────────────
df_marketing = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base}/raw/marketing/marketing_campaigns.csv"))

display(df_marketing)
# print("\n✓ All 4 sources loaded successfully!")