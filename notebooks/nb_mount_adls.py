storage_account = "saetlpipeline100"
container       = "datalake100"        

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="adls-storage-key")
)

# List everything inside the container
base = dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/")
for b in base:
    print(b.path+"\n")

# Check raw/sales/
print("=== raw/sales/ ===")
sales_files = dbutils.fs.ls(f"abfss://datalake100@saetlpipeline100.dfs.core.windows.net/raw/sales/")
for f in sales_files:
    print(f.path)

# Check raw/master/
print("\n=== raw/master/ ===")
master_files = dbutils.fs.ls(f"abfss://datalake100@saetlpipeline100.dfs.core.windows.net/raw/master/")
for f in master_files:
    print(f.path)
    
# Check raw/support/
print("\n=== raw/support/ ===")
support_files = dbutils.fs.ls(f"abfss://datalake100@saetlpipeline100.dfs.core.windows.net/raw/support/")
for f in support_files:
    print(f.path)

# Check raw/marketing/
print("\n=== raw/marketing/ ===")
marketing_files = dbutils.fs.ls(f"abfss://datalake100@saetlpipeline100.dfs.core.windows.net/raw/marketing/")
for f in marketing_files:
    print(f.path)
