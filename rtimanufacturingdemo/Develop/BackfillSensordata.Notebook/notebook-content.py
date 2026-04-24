# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b37f74c5-95dd-4ec4-b36b-755ae80c51f3",
# META       "default_lakehouse_name": "ManufacturingData",
# META       "default_lakehouse_workspace_id": "dfca7324-366b-4292-9e43-a869101584d2",
# META       "known_lakehouses": [
# META         {
# META           "id": "b37f74c5-95dd-4ec4-b36b-755ae80c51f3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import (
    date_sub, col, year, month, dayofmonth, 
    floor, rand, make_timestamp
)
from pyspark.sql.functions import date_format, col

df = spark.sql("SELECT * FROM ManufacturingData.machinedata.sensors_parsed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

timewarp = 365 + 25
for i in range(1,timewarp):
    # Modify timestamp: go back {timewarp} days and set random time between 6am-10pm
    df_modified = df.withColumn(
        "date_adjusted",
        date_sub(col("timestamp").cast("date"), i)
    ).withColumn(
        "random_hour",
        (floor(rand() * 17) + 6).cast("int")  # 6-22 (6am-10pm)
    ).withColumn(
        "random_minute",
        floor(rand() * 60).cast("int")
    ).withColumn(
        "random_second",
        floor(rand() * 60).cast("int")
    ).withColumn(
        "timestamp",
        make_timestamp(
            year(col("date_adjusted")),
            month(col("date_adjusted")),
            dayofmonth(col("date_adjusted")),
            col("random_hour"),
            col("random_minute"),
            col("random_second")
        )
    ).drop("date_adjusted", "random_hour", "random_minute", "random_second")

    # Show the results

    # Recalculate Date and Time columns from the modified timestamp
    df_final = df_modified.withColumn(
        "Date",
        date_format(col("timestamp"), "yyyy-MM-dd")
    ).withColumn(
        "Time",
        date_format(col("timestamp"), "HH:mm:ss")
    )

    # Verify the results
    df_final.write.mode("append").format("delta").saveAsTable("dbo.sensor_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM ManufacturingData.dbo.sensor_data")
all_count = df.count()
print(all_count)
df = df.dropDuplicates(["timestamp","machine_id","site_id"])
without_duplicates = df.count()
print(without_duplicates)
if all_count > without_duplicates:
    df.write.mode("overwrite").format("delta").saveAsTable("dbo.sensor_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
