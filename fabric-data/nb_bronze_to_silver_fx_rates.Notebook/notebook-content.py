# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2d75f693-d076-4974-b963-1c739ba472a4",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "7b50e941-562a-45d8-84ca-2f60673e114d",
# META       "known_lakehouses": [
# META         {
# META           "id": "2d75f693-d076-4974-b963-1c739ba472a4"
# META         },
# META         {
# META           "id": "366e598f-5f6e-45ff-8358-b2d0268dc08f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze → Silver: FX rates
# 
# Fix the rate column name, add a `date_key`, and write the ECB USD/EUR daily rates to silver as Delta.

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Inspect the source
# 
# Check the date range, rate range, nulls, and duplicates. Source is already clean and typed.

# CELL ********************

df_ecb = spark.sql("SELECT * FROM project.lh_bronze.dbo.ecb_usd_eur_daily")

print(f"Rows: {df_ecb.count()}")
df_ecb.printSchema()


df_ecb.select(
    F.min("rate_date").alias("earliest"),
    F.max("rate_date").alias("latest"),
    F.min("usd_eur_rate").alias("min_rate"),
    F.max("usd_eur_rate").alias("max_rate"),
    F.round(F.avg("usd_eur_rate"), 4).alias("avg_rate"),
).show()


for c in df_ecb.columns:
    n = df_ecb.filter(F.col(c).isNull()).count()
    print(f"  {c}: {n} NULL")


total = df_ecb.count()
unique = df_ecb.select("rate_date").distinct().count()
print(f"\nDuplicates: {total - unique}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform and write to silver
# 
# Rename `usd_eur_rate` → `eur_usd_rate` (the column holds USD per 1 EUR), add `date_key` for joining to a date dimension, then save as a Delta table. Row count is unchanged.

# CELL ********************

df_ecb_silver = (
    df_ecb
    # source column is named usd_eur_rate but holds USD per 1 EUR (i.e. EUR/USD), so rename
    .withColumnRenamed("usd_eur_rate", "eur_usd_rate")
    .withColumn("date_key", F.date_format("rate_date", "yyyyMMdd").cast("int"))
)

# before / after
print(f"rows    : {df_ecb.count()} -> {df_ecb_silver.count()}")
print(f"columns : {df_ecb.columns} -> {df_ecb_silver.columns}")
df_ecb_silver.show(5)

df_ecb_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_fx_rates")

print("silver_fx_rates written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
