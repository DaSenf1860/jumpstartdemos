# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080",
# META       "default_lakehouse_name": "manufacturing_data",
# META       "default_lakehouse_workspace_id": "ce753ac1-7233-4889-b54d-f0ca9df04e06",
# META       "known_lakehouses": [
# META         {
# META           "id": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql.functions import (
    col, date_sub, year, month, dayofmonth,
    make_timestamp, to_timestamp, hour, minute, second,
    current_date, current_timestamp, date_format
)
import random

df = spark.read.format("parquet").load("Files/data/productionquality")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

timewarp = 365

# Today (UTC) and current time as HH:mm:ss (UTC)
today = current_date()
current_time_utc = date_format(current_timestamp(), "HH:mm:ss")

for i in range(timewarp):
    # pick a random source day (1..10)
    random_day = random.randint(1, 10)

    # base filter: pick that source day
    df_ = df.filter(col("Date") == random_day)

    # for today (i == 0), only keep times in the past (UTC)
    if i == 0:
        df_ = df_.filter(col("Time") < current_time_utc)

    # if no rows, skip this iteration
    if df_.rdd.isEmpty():
        continue

    df_modified = (
        df_
        # new date is "today - i days"
        .withColumn("date_adjusted", date_sub(today, i))
        # parse Time (HH:mm:ss) once
        .withColumn("time_ts", to_timestamp(col("Time"), "HH:mm:ss"))
        # new timestamp = adjusted date + original time-of-day
        .withColumn(
            "timestamp",
            make_timestamp(
                year(col("date_adjusted")),
                month(col("date_adjusted")),
                dayofmonth(col("date_adjusted")),
                hour(col("time_ts")),
                minute(col("time_ts")),
                second(col("time_ts"))
            )
        )
        .drop("date_adjusted", "time_ts")
    )

    # e.g. append to your target table
    df_modified.write.mode("append").format("delta").saveAsTable("dbo.production_quality")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM manufacturing_data.dbo.production_quality")
all_count = df.count()
df = df.dropDuplicates(["timestamp","machine_id","site_id"])
without_duplicates = df.count()
print(all_count, without_duplicates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if all_count > without_duplicates:
    df.write.format("delta").mode("overwrite").saveAsTable("manufacturing_data.dbo.production_quality")

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
