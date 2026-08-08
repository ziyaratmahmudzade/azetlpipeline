storage_account = "saetlpipeline100"
container       = "datalake100"        

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)
base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
# ── Read CSV ──────────────────────────────────────────────────────
df_sales = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base}/raw/sales/sales_transactions.csv"))

print(f"Sales rows: {df_sales.count()}")
display(df_sales)

# ── Read XLSX (Customers sheet) ───────────────────────────────────
df_customers = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True)
    .option("inferSchema", True)
    .option("dataAddress", "'Customers'!A1")
    .load(f"{base}/raw/master/master_data.xlsx"))

print(f"Customer rows: {df_customers.count()}")
display(df_customers)

# ── Read XLSX (Products sheet) ────────────────────────────────────
df_products = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True)
    .option("inferSchema", True)
    .option("dataAddress", "'Products'!A1")
    .load(f"{base}/raw/master/master_data.xlsx"))

print(f"Product rows: {df_products.count()}")
display(df_products)
