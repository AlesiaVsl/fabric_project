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

# CELL ********************

from pyspark.sql import functions as F

# Читаем из Bronze
df_loc = spark.sql("SELECT * FROM project.lh_bronze.dbo.openaq_locations")

print(f"Bronze rows: {df_loc.count()}")
df_loc.printSchema()
df_loc.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = df_loc \
    .filter(F.col("sensor_id").isNotNull()) \
    .filter(F.col("location_name").isNotNull()) \
    .select(
        "sensor_id",
        "location_id",
        "location_name",
        "locality",
        "latitude",
        "longitude",
        "parameter_name",
        "units"
    ) \
    .dropDuplicates(["sensor_id"])

print(f"Silver rows after cleanup: {df_silver.count()}")
df_silver.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_sensor_locations")

print("silver_sensor_locations saved")
spark.sql("SHOW TABLES LIKE 'silver_sensor_locations'").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
