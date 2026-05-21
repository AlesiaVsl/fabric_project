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

# # Silver layer — verification & zones
# 
# Check all four silver tables came out clean and join-ready, then build the taxi zones lookup table.

# CELL ********************

from pyspark.sql import functions as F

BRONZE_FILES = "abfss://project@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Table summary
# 
# Row counts, column counts, and schema for each silver table.

# CELL ********************

tables = ["silver_taxi_trips", "silver_fx_rates", "silver_gdp", "silver_air_quality"]

print("=" * 70)
print("SILVER LAYER — SUMMARY")
print("=" * 70)

for t in tables:
    df = spark.table(t)
    print(f"\n {t}: {df.count():,} rows, {len(df.columns)} columns")
    df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Date coverage & join compatibility
# 
# Date ranges per table, plus how many `date_key` values overlap — the basis for joining in gold.

# CELL ********************

print("=== Date ranges ===")

ranges = [
    ("silver_taxi_trips", "pickup_date"),
    ("silver_fx_rates", "rate_date"),
    ("silver_air_quality", "measurement_date"),
]

for t, c in ranges:
    df = spark.table(t)
    df.select(
        F.lit(t).alias("table"),
        F.min(c).alias("from"),
        F.max(c).alias("to"),
        F.countDistinct(c).alias("distinct_days"),
    ).show(truncate=False)

print("=== Join compatibility (date_key overlap) ===")
taxi_days = spark.table("silver_taxi_trips").select("date_key").distinct()
fx_days = spark.table("silver_fx_rates").select("date_key").distinct()
aq_days = spark.table("silver_air_quality").select("date_key").distinct()

print(f"  taxi distinct days: {taxi_days.count()}")
print(f"  taxi ∩ fx:          {taxi_days.join(fx_days, 'date_key', 'inner').count()} (gaps = weekends/holidays)")
print(f"  taxi ∩ air_quality: {taxi_days.join(aq_days, 'date_key', 'inner').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verify: taxi trips
# 
# Confirm every filter held — nulls, negatives, caps, duration, zone IDs, dedup, date_key, and the passenger flag. Every check should be 0.

# CELL ********************

df = spark.table("silver_taxi_trips")
total = df.count()
expected = 79_174_102
print(f"silver_taxi_trips: {total:,} rows (expected {expected:,})")
print(f"  match: {'OK' if total == expected else 'MISMATCH'}")

checks = {
    "NULL pickup_datetime":   df.filter(F.col("pickup_datetime").isNull()).count(),
    "NULL total_amount":      df.filter(F.col("total_amount").isNull()).count(),
    "NULL pickup_location":   df.filter(F.col("pickup_location_id").isNull()).count(),
    "negative distance":      df.filter(F.col("trip_distance") < 0).count(),
    "negative total_amount":  df.filter(F.col("total_amount") < 0).count(),
    "negative fare_amount":   df.filter(F.col("fare_amount") < 0).count(),
    "negative passengers":    df.filter(F.col("passenger_count") < 0).count(),
    "total_amount > 200":     df.filter(F.col("total_amount") > 200).count(),
    "trip_distance > 50":     df.filter(F.col("trip_distance") > 50).count(),
    "duration <= 0":          df.filter(F.col("duration_min") <= 0).count(),
    "duration > 180":         df.filter(F.col("duration_min") > 180).count(),
    "zero trips":             df.filter((F.col("trip_distance")==0) & (F.col("total_amount")==0)).count(),
    "pickup_location < 1":    df.filter(F.col("pickup_location_id") < 1).count(),
    "pickup_location > 263":  df.filter(F.col("pickup_location_id") > 263).count(),
    "dropoff_location < 1":   df.filter(F.col("dropoff_location_id") < 1).count(),
    "dropoff_location > 263": df.filter(F.col("dropoff_location_id") > 263).count(),
    "date < 2024-01-01":      df.filter(F.col("pickup_date") < "2024-01-01").count(),
    "date > 2026-03-31":      df.filter(F.col("pickup_date") > "2026-03-31").count(),
    "duplicates":             total - df.distinct().count(),
    "date_key mismatch":      df.filter(F.date_format("pickup_date", "yyyyMMdd").cast("int") != F.col("date_key")).count(),
    "is_passenger_trip wrong": df.filter(
        ((F.col("passenger_count") > 0) & (F.col("is_passenger_trip") == False)) |
        ((F.col("passenger_count") == 0) & (F.col("is_passenger_trip") == True))
    ).count(),
}
failed = {k: v for k, v in checks.items() if v != 0}
for label, n in checks.items():
    print(f"  {'OK  ' if n == 0 else 'FAIL'} {label}: {n}")
print(f"\nresult: {'all checks passed' if not failed else str(len(failed)) + ' check(s) failed'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verify: FX rates
# 
# Nulls, plausible rate range, and duplicate dates. `date_key` should be int.

# CELL ********************

df_fx = spark.table("silver_fx_rates")
print(f"silver_fx_rates: {df_fx.count()} rows (expected 594)")

fx_checks = {
    "NULL rate":          df_fx.filter(F.col("eur_usd_rate").isNull()).count(),
    "NULL date":          df_fx.filter(F.col("rate_date").isNull()).count(),
    "NULL date_key":      df_fx.filter(F.col("date_key").isNull()).count(),
    "rate < 0.5":         df_fx.filter(F.col("eur_usd_rate") < 0.5).count(),
    "rate > 2.0":         df_fx.filter(F.col("eur_usd_rate") > 2.0).count(),
    "duplicate dates":    df_fx.count() - df_fx.select("rate_date").distinct().count(),
}
for label, n in fx_checks.items():
    print(f"  {'OK  ' if n == 0 else 'WARN'} {label}: {n}")
print(f"  date_key type: {dict(df_fx.dtypes)['date_key']} (expected int)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verify: GDP
# 
# Nulls, year bounds, types, and a continuous 2020–2024 series.

# CELL ********************

df_gdp = spark.table("silver_gdp")
print(f"silver_gdp: {df_gdp.count()} rows (expected 5)")

gdp_checks = {
    "NULL year":   df_gdp.filter(F.col("year").isNull()).count(),
    "NULL gdp":    df_gdp.filter(F.col("gdp_current_usd").isNull()).count(),
    "year < 2020": df_gdp.filter(F.col("year") < 2020).count(),
    "year > 2024": df_gdp.filter(F.col("year") > 2024).count(),
}
for label, n in gdp_checks.items():
    print(f"  {'OK  ' if n == 0 else 'FAIL'} {label}: {n}")

print(f"  year type: {dict(df_gdp.dtypes)['year']} (expected int)")
print(f"  gdp_current_usd type: {dict(df_gdp.dtypes)['gdp_current_usd']} (expected double)")

years = sorted([r[0] for r in df_gdp.select("year").collect()])
print(f"  years: {years}")
print(f"  {'OK  ' if years == list(range(2020, 2025)) else 'FAIL'} continuous 2020-2024")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verify: air quality
# 
# Nulls, no negatives, dedup on (date + sensor + parameter), and domain sanity on pollutant values.

# CELL ********************

df_aq = spark.table("silver_air_quality")
print(f"silver_air_quality: {df_aq.count():,} rows (expected 9,441)")

aq_checks = {
    "NULL value":            df_aq.filter(F.col("value").isNull()).count(),
    "negative value":        df_aq.filter(F.col("value") < 0).count(),
    "NULL measurement_date": df_aq.filter(F.col("measurement_date").isNull()).count(),
    "NULL sensor_id":        df_aq.filter(F.col("sensor_id").isNull()).count(),
    "NULL parameter_name":   df_aq.filter(F.col("parameter_name").isNull()).count(),
    "duplicates (date+sensor+param)": df_aq.count() - df_aq.select("measurement_date", "sensor_id", "parameter_name").distinct().count(),
}
for label, n in aq_checks.items():
    print(f"  {'OK  ' if n == 0 else 'FAIL'} {label}: {n}")

params = sorted([r[0] for r in df_aq.select("parameter_name").distinct().collect()])
print(f"  parameters: {params}")

pm25_extreme = df_aq.filter((F.col("parameter_name")=="pm25") & (F.col("value") > 500)).count()
no2_extreme  = df_aq.filter((F.col("parameter_name")=="no2")  & (F.col("value") > 5)).count()
print(f"  {'OK  ' if pm25_extreme == 0 else 'WARN'} PM2.5 > 500: {pm25_extreme}")
print(f"  {'OK  ' if no2_extreme == 0 else 'WARN'} NO2 > 5: {no2_extreme}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build the taxi zones lookup
# 
# Read the TLC zone CSV, rename to snake_case, type `location_id` as int, and write `silver_taxi_zones`.

# CELL ********************

from pyspark.sql import functions as F

df_zones = spark.read.option("header", "true").csv(
    "abfss://project@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/nyc_taxi/zones/taxi_zone_lookup.csv"
)

df_zones.printSchema()
df_zones.show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write zones to silver

# CELL ********************

from pyspark.sql import functions as F

df_zones = spark.read.option("header", "true").csv(
    "abfss://project@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/nyc_taxi/zones/taxi_zone_lookup.csv"
)

df_zones_silver = df_zones \
    .withColumnRenamed("LocationID", "location_id") \
    .withColumnRenamed("Borough", "borough") \
    .withColumnRenamed("Zone", "zone_name") \
    .withColumn("location_id", F.col("location_id").cast("int")) \
    .filter(F.col("location_id").isNotNull())

df_zones_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("silver_taxi_zones")

print(f"silver_taxi_zones: {df_zones_silver.count()} rows")
df_zones_silver.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
