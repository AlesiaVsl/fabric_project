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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


from pyspark.sql import functions as F

df_taxi = spark.read.format("delta").load("Tables/silver_taxi_trips")
print(f"Source rows: {df_taxi.count():,}")


df_hourly = (
    df_taxi
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("day_of_week", F.dayofweek("pickup_date"))  
    .groupBy(
        F.col("pickup_date").alias("full_date"),
        F.col("date_key"),
        F.col("pickup_hour"),
        F.col("day_of_week"),
        F.col("pickup_location_id"),
        F.col("year"),
        F.col("month")
    )
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue_usd"),
        F.sum("passenger_count").alias("total_passengers"),
        F.sum("trip_distance").alias("total_distance"),
        F.avg("total_amount").alias("avg_fare_usd"),
        F.avg("duration_min").alias("avg_duration_min"),
        F.avg("trip_distance").alias("avg_distance")
    )
)

df_hourly.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("silver_taxi_hourly")

print(f"silver_taxi_hourly: {df_hourly.count():,} rows")
df_hourly.show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
