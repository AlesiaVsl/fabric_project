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

# # Bronze → Silver: NYC taxi trips
# 
# Clean and filter the yellow taxi parquet files, then write to silver as Delta. The cleaning runs in stages so each step's row impact is visible.

# CELL ********************

from pyspark.sql import functions as F

BRONZE_FILES = "abfss://project@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read the raw parquet
# 
# Load the yellow taxi files from bronze and check the shape and schema.

# CELL ********************

df_taxi_raw = spark.read.parquet(f"{BRONZE_FILES}/nyc_taxi/yellow")

print(f"Raw rows: {df_taxi_raw.count():,}")
print(f"Columns: {len(df_taxi_raw.columns)}")
df_taxi_raw.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Profile before cleaning
# 
# Look at the distribution of `total_amount` and `trip_distance`. The long tail sets the outlier caps used later.

# CELL ********************

# Percentiles for total_amount — reveals the long tail of outliers
print("=== TOTAL_AMOUNT distribution ===")
df_taxi_raw.select("total_amount").summary(
    "min", "25%", "50%", "75%", "90%", "95%", "99%", "max"
).show()

# Percentiles for trip_distance
print("=== TRIP_DISTANCE distribution ===")
df_taxi_raw.select("trip_distance").summary(
    "min", "25%", "50%", "75%", "90%", "95%", "99%", "max"
).show()

# Observation:
#   total_amount: 99% of trips are <= $106, but max = $863,380 → set cap at $200
#   trip_distance: 99% of trips are <= 20 miles, but max = 186,967 → set cap at 50 miles

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Select and rename
# 
# Keep the columns we need and give them snake_case names.

# CELL ********************

# Select + rename columns
df_taxi = df_taxi_raw.select(
    F.col("VendorID").alias("vendor_id"),
    F.col("tpep_pickup_datetime").alias("pickup_datetime"),
    F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
    F.col("passenger_count"),
    F.col("trip_distance"),
    F.col("PULocationID").alias("pickup_location_id"),
    F.col("DOLocationID").alias("dropoff_location_id"),
    F.col("payment_type"),
    F.col("fare_amount"),
    F.col("tip_amount"),
    F.col("tolls_amount"),
    F.col("total_amount"),
    F.col("congestion_surcharge"),
    F.col("Airport_fee").alias("airport_fee"),
    F.col("year"),
    F.col("month"),
)

df_taxi.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Drop invalid rows
# 
# Remove nulls in critical columns and physically impossible values (negative distance, fare, etc.).

# CELL ********************

# Drop rows with NULLs in critical columns + physically impossible values
before = df_taxi.count()

df_taxi = df_taxi.filter(
    F.col("pickup_datetime").isNotNull()
    & F.col("dropoff_datetime").isNotNull()
    & F.col("pickup_location_id").isNotNull()
    & F.col("total_amount").isNotNull()
).filter(
    (F.col("trip_distance") >= 0)
    & (F.col("total_amount") >= 0)
    & (F.col("fare_amount") >= 0)
    & (F.col("passenger_count") >= 0)
)

after = df_taxi.count()
print(f"rows : {before:,} -> {after:,}  ({before - after:,} dropped)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enrich and deduplicate
# 
# Add `pickup_date` + `date_key`, then drop exact duplicate rows.

# CELL ********************

# Add date columns, then drop exact duplicate rows
# (checked: only full-row dupes are real here; business-key matches are
#  legitimate distinct trips, so we dedupe on the full row, not a key)
df_taxi = (
    df_taxi
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
)

before = df_taxi.count()
df_taxi = df_taxi.dropDuplicates()
after = df_taxi.count()
print(f"rows : {before:,} -> {after:,}  ({before - after:,} duplicates dropped)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Remove outliers
# 
# Apply the date range, the percentile caps from profiling, a sane trip-duration window, and valid TLC TaxiZone IDs (1–263).

# CELL ********************

# Remove outliers: date range + percentile caps + trip duration + valid TaxiZone IDs
before = df_taxi.count()

df_taxi = df_taxi.withColumn(
    "duration_min",
    (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
).filter(
    (F.col("pickup_date") >= "2024-01-01")
    & (F.col("pickup_date") <= "2026-03-31")
    & (F.col("total_amount") <= 200)        # 99% of trips <= $106; cap tail
    & (F.col("trip_distance") <= 50)        # 99% of trips <= 20 mi; cap tail
    & (F.col("duration_min") > 0)
    & (F.col("duration_min") <= 180)
    & ~((F.col("trip_distance") == 0) & (F.col("total_amount") == 0))
).filter(
    (F.col("pickup_location_id") >= 1) & (F.col("pickup_location_id") <= 263)
    & (F.col("dropoff_location_id") >= 1) & (F.col("dropoff_location_id") <= 263)
)

after = df_taxi.count()
print(f"rows : {before:,} -> {after:,}  ({before - after:,} outliers removed)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Flag and finalize
# 
# Flag zero-passenger trips rather than dropping them, then check the final schema.

# CELL ********************

# Flag zero-passenger trips instead of dropping them
df_taxi = df_taxi.withColumn(
    "is_passenger_trip", F.when(F.col("passenger_count") > 0, True).otherwise(False)
)

print(f"final rows : {df_taxi.count():,}")
df_taxi.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to silver
# 
# Save the cleaned dataset as a Delta table.

# CELL ********************

df_taxi.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_taxi_trips")

print("silver_taxi_trips written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
