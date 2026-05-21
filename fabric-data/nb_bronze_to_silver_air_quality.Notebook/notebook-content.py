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

# # Bronze → Silver: NYC air quality
# 
# Clean the OpenAQ daily readings (no2 + pm25) and write to silver as Delta.

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
# Profile parameters, units, and value ranges. Check for negative readings before filtering.

# CELL ********************

df_aq = spark.sql("SELECT * FROM project.lh_bronze.dbo.openaq_nyc_daily")

print(f"Rows: {df_aq.count()}")
df_aq.printSchema()
df_aq.show(5, truncate=False)


print("\n=== Parameters ===")
df_aq.groupBy("parameter_name", "units").count().show()

print("=== Value ranges ===")
df_aq.groupBy("parameter_name").agg(
    F.min("value").alias("min"),
    F.max("value").alias("max"),
    F.round(F.avg("value"), 4).alias("avg"),
    F.count("value").alias("rows"),
).show()

neg = df_aq.filter(F.col("value") < 0).count()
print(f"Negative values (will be removed): {neg}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform and write to silver
# 
# Drop negative values, parse `measurement_date` + `date_key` from the UTC string, dedup per sensor/day/parameter, and save as a Delta table. Before/after shows what each step removed.

# CELL ********************

df_filtered = df_aq.filter(F.col("value") >= 0)

df_aq_silver = (
    df_filtered
    .withColumn("measurement_date", F.to_date(F.substring("utc", 1, 10)))
    .withColumn("date_key", F.date_format("measurement_date", "yyyyMMdd").cast("int"))
    .withColumn("sensor_id", F.col("sensor_id").cast("int"))
    .select(
        "measurement_date",
        "date_key",
        "sensor_id",
        "parameter_name",
        "value",
        "units",
    )
    .dropDuplicates(["measurement_date", "sensor_id", "parameter_name"])
)

# before / after
print(f"rows     : {df_aq.count()} -> {df_aq_silver.count()}")
print(f"filter   : {df_aq.count() - df_filtered.count()} negatives dropped")
print(f"dedup    : {df_filtered.count() - df_aq_silver.count()} duplicate rows dropped")
print(f"date     : utc string -> {dict(df_aq_silver.dtypes)['measurement_date']} + date_key int")
df_aq_silver.show(5, truncate=False)

df_aq_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_air_quality")

print("silver_air_quality written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
