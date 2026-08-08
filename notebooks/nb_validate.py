from pyspark.sql.functions import col

storage_account = "saetlpipeline100"
container       = "datalake100"
base            = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)

# ── Load curated Delta tables ─────────────────────────────────────
df_t = spark.read.format("delta").load(f"{base}/curated/transactions/")
df_c = spark.read.format("delta").load(f"{base}/curated/customers/")
df_p = spark.read.format("delta").load(f"{base}/curated/products/")

# ── Run quality checks ────────────────────────────────────────────
checks = []
checks.append(("Row count > 0 (transactions)", df_t.count() > 0))
checks.append(("Row count > 0 (customers)",    df_c.count() > 0))
checks.append(("Row count > 0 (products)",     df_p.count() > 0))
checks.append(("No null transaction_id",       df_t.filter(col("transaction_id").isNull()).count() == 0))
checks.append(("No null customer_id",          df_t.filter(col("customer_id").isNull()).count() == 0))
checks.append(("No negative amounts",          df_t.filter(col("amount") <= 0).count() == 0))
checks.append(("No cancelled orders",          df_t.filter(col("status") == "CANCELLED").count() == 0))
checks.append(("All customers have segment",   df_c.filter(col("segment").isNull()).count() == 0))

# ── Print report ──────────────────────────────────────────────────
print("=" * 45)
print("DATA QUALITY REPORT")
print("=" * 45)
all_passed = True
for name, result in checks:
    status = "✓ PASS" if result else "✗ FAIL"
    if not result:
        all_passed = False
    print(f"{status}  {name}")

print("=" * 45)
if all_passed:
    print("ALL CHECKS PASSED — data is ready for Synapse")
else:
    raise Exception("DATA QUALITY CHECKS FAILED — pipeline stopped")
