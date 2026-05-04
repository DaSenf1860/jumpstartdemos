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
print("🚀 Creating Data for historical analysis")
df = spark.read.format("parquet").load("Files/data/production_quality")
print(f"✅ Success: reading sample data")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

timewarp = 10

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
        .withColumn("Date", date_format(date_sub(today, i), "yyyy-MM-dd"))
        # parse Time (HH:mm:ss) once
        .withColumn("time_ts", to_timestamp(col("Time"), "HH:mm:ss"))
        # new timestamp = adjusted date + original time-of-day
        .withColumn(
            "timestamp",
            make_timestamp(
                year(col("Date")),
                month(col("Date")),
                dayofmonth(col("Date")),
                hour(col("time_ts")),
                minute(col("time_ts")),
                second(col("time_ts"))
            )
        )
        .drop("time_ts")
    )

    # e.g. append to your target table
    df_modified.write.mode("append").format("delta").saveAsTable("dbo.production_quality")
    print(f"✅ Success: Creating data for date: t-{i}")



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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if all_count > without_duplicates:
    df.write.format("delta").mode("overwrite").saveAsTable("manufacturing_data.dbo.production_quality")
    print(f"✅ Success: Cleaning up duplicates")
print(f"✅ Success: No duplicates")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime, timedelta

df = spark.sql("SELECT * FROM manufacturing_data.dbo.production_quality")

max_date_hour = df.agg(F.max("timestamp").alias("max_date_hour")).collect()[0][0]
end_date = max_date_hour

spark.sql("DROP TABLE IF EXISTS manufacturing_data.dbo.dim_date")
start_date = datetime(2026, 1, 1)

date_list = [(start_date + timedelta(hours=x),) for x in range(1, 24*(end_date - start_date).days + 24)]
    
# Create DataFrame from date list
df_dates = spark.createDataFrame(date_list, ["date"])

# Create dimension table with date attributes

# Create dimension table with date attributes
dim_date = df_dates.select(
    F.date_format(F.col("date"), "yyyy-MM-dd").cast("string").alias("date_id"),
    F.year("date").alias("year"),
    F.concat(F.year("date"), F.lit("-"), F.month("date")).alias("year_month"),
    F.dayofmonth("date").alias("day"),
    F.date_format("date", "yyyy-MM-dd HH:00").alias("date_hour"),
    F.dayofweek("date").alias("day_of_week"),
    F.concat(F.year("date"), F.lit("-W"), F.weekofyear("date")).alias("year_week"),
    F.concat(F.year("date"), F.lit("-Q"), F.quarter("date")).alias("year_quarter"),
    F.concat(F.year("date"), F.lit("-"), F.dayofyear("date")).alias("year_day_of_year"),
    F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
    F.date_format("date", "EEEE").alias("day_name"),
    F.date_format("date", "MMMM yyyy").alias("year_month_name"),
    F.when((F.hour("date") >= 6) & (F.hour("date") < 12), "morning")
        .when((F.hour("date") >= 12) & (F.hour("date") < 18), "afternoon")
        .otherwise("night").alias("shift")
).orderBy("date_id")


#dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("dbo.dim_date")
dim_date.write.format("delta").mode("append").saveAsTable("dbo.dim_date")

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
