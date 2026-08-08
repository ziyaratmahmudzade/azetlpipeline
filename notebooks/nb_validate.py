from pyspark.sql.functions import col

storage_account = "saetlpipeline100"
container       = "datalake100"
base            = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)

# ── Load all 5 curated Delta tables ──────────────────────────────
df_t = spark.read.format("delta").load(f"{base}/curated/transactions/")
df_c = spark.read.format("delta").load(f"{base}/curated/customers/")
df_p = spark.read.format("delta").load(f"{base}/curated/products/")
df_s = spark.read.format("delta").load(f"{base}/curated/support/")
df_m = spark.read.format("delta").load(f"{base}/curated/marketing/")

# ── Run quality checks ────────────────────────────────────────────
checks = []

# Transactions
checks.append(("Transactions — row count > 0",         df_t.count() > 0))
checks.append(("Transactions — no null transaction_id", df_t.filter(col("transaction_id").isNull()).count() == 0))
checks.append(("Transactions — no null customer_id",    df_t.filter(col("customer_id").isNull()).count() == 0))
checks.append(("Transactions — no negative amounts",    df_t.filter(col("amount") <= 0).count() == 0))
checks.append(("Transactions — no cancelled orders",    df_t.filter(col("status") == "CANCELLED").count() == 0))
checks.append(("Transactions — has customer_segment",   df_t.filter(col("customer_segment").isNull()).count() == 0))

# Customers
checks.append(("Customers — row count > 0",             df_c.count() > 0))
checks.append(("Customers — no null customer_id",       df_c.filter(col("customer_id").isNull()).count() == 0))
checks.append(("Customers — all have segment",          df_c.filter(col("segment").isNull()).count() == 0))
checks.append(("Customers — all have industry",         df_c.filter(col("industry").isNull()).count() == 0))

# Products
checks.append(("Products — row count > 0",              df_p.count() > 0))
checks.append(("Products — no negative margin",         df_p.filter(col("margin_pct") < 0).count() == 0))
checks.append(("Products — no null product_name",       df_p.filter(col("product_name").isNull()).count() == 0))

# Support
checks.append(("Support — row count > 0",               df_s.count() > 0))
checks.append(("Support — no null customer_id",         df_s.filter(col("customer_id").isNull()).count() == 0))
checks.append(("Support — no null ticket_id",           df_s.filter(col("ticket_id").isNull()).count() == 0))

# Marketing
checks.append(("Marketing — row count > 0",             df_m.count() > 0))
checks.append(("Marketing — no null campaign_id",       df_m.filter(col("campaign_id").isNull()).count() == 0))
checks.append(("Marketing — no negative budget",        df_m.filter(col("budget_usd") <= 0).count() == 0))

# ── Print report ──────────────────────────────────────────────────
print("=" * 55)
print("DATA QUALITY REPORT — ALL 5 TABLES")
print("=" * 55)
all_passed = True
for name, result in checks:
    status = "✓ PASS" if result else "✗ FAIL"
    if not result:
        all_passed = False
    print(f"{status}  {name}")

print("=" * 55)
if all_passed:
    print("ALL CHECKS PASSED — data is ready for Synapse")
else:
    raise Exception("DATA QUALITY CHECKS FAILED — pipeline stopped")
