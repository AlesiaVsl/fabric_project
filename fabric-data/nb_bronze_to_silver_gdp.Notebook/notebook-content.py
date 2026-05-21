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

# # Bronze → Silver: USA GDP
# 
# Clean the World Bank GDP table from bronze and write it to silver as Delta.

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
# Load the bronze table and check row count, schema, and nulls before transforming.

# CELL ********************

df_gdp = spark.sql("SELECT * FROM project.lh_bronze.dbo.worldbank_usa_gdp")

print(f"Rows: {df_gdp.count()}")
df_gdp.printSchema()
df_gdp.orderBy("year").show()

for c in df_gdp.columns:
    n = df_gdp.filter(F.col(c).isNull()).count()
    print(f"  {c}: {n} NULL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform and write to silver
# 
# Drop the null GDP row (2025 not reported yet), cast `year` to int, and save as a Delta table. Quick before/after to confirm the change.

# CELL ********************

df_gdp_silver = (
    df_gdp
    .filter(F.col("gdp_current_usd").isNotNull())
    .withColumn("year", F.col("year").cast("int"))
)

# before / after
print(f"rows : {df_gdp.count()} -> {df_gdp_silver.count()}")
print(f"year : {dict(df_gdp.dtypes)['year']} -> {dict(df_gdp_silver.dtypes)['year']}")
df_gdp_silver.orderBy("year").show()

df_gdp_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_gdp")

print("silver_gdp written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
