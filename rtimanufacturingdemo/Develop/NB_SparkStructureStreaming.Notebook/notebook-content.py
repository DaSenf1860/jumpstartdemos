# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080",
# META       "default_lakehouse_name": "ManufacturingData",
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
# Spark Structured Streaming: OneLake Delta to Lakehouse Append
from time import sleep
from pyspark.sql.streaming import StreamingQueryException
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window



spark.conf.set('spark.sql.parquet.vorder.default', 'true')

# Configuration
jobs = [{"source_table": "production_quality",
        "target_schema": "dbo",
        "target_table": "production_quality"},
        {"source_table": "sensors_parsed",
        "target_schema": "dbo",
        "target_table": "sensor_data"},
]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def stream_yo(source_table, target_schema, target_table):

    SOURCE_ONELAKE_PATH = f"abfss://manufacturingdemo@onelake.dfs.fabric.microsoft.com/ManufacturingData.Lakehouse/Tables/machinedata/{source_table}"

    # Variables to store min and max timestamps
    timestamps = {"min": None, "max": None, "count": 0}

    def process_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            # Get min and max timestamps from this batch
            stats = batch_df.agg(
                {"timestamp": "min", "timestamp": "max"}
            ).collect()[0]
            
            # For multiple aggregations on same column, use this approach:
            from pyspark.sql.functions import min as spark_min, max as spark_max
            stats = batch_df.select(
                spark_min("timestamp").alias("min_ts"),
                spark_max("timestamp").alias("max_ts")
            ).collect()[0]
            
            batch_min = stats["min_ts"]
            batch_max = stats["max_ts"]
            
            if timestamps["min"] is None or batch_min < timestamps["min"]:
                timestamps["min"] = batch_min
            if timestamps["max"] is None or batch_max > timestamps["max"]:
                timestamps["max"] = batch_max
            
            timestamps["count"] += batch_df.count()
            
            # Write the batch to the target table
            batch_df.write.format("delta").mode("append").saveAsTable(f"{target_schema}.{target_table}")

    # Read streaming data from OneLake Delta table
    df_stream = (spark
        .readStream
        .format("delta")
        .load(SOURCE_ONELAKE_PATH)
    )

    # Write stream using foreachBatch
    query = (df_stream
        .writeStream
        .foreachBatch(process_batch)
        .trigger(availableNow=True)
        .option("checkpointLocation", f"Files/checkpoints/{source_table}_{target_table}/_checkpoint/streaming_append")
        .start()
    )
    # Wait for termination (set timeout as needed)
    query.awaitTermination(timeout=None)  # Remove timeout to run indefinitely

    return timestamps


    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_date_table():
    df = spark.sql("SELECT * FROM ManufacturingData.dbo.production_quality")

    max_date_hour = df.agg(F.max("timestamp").alias("max_date_hour")).collect()[0][0]
    end_date = max_date_hour

    try:
        df_date = spark.sql("SELECT * FROM ManufacturingData.dbo.dim_date")
        start_date = df_date.agg(F.max("date_hour").alias("max_date_hour")).collect()[0][0]
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M")
    except:
        spark.sql("DROP TABLE IF EXISTS ManufacturingData.dbo.dim_date")
        start_date = datetime(2025, 1, 1)

    if start_date >= end_date:
        print("Up to date")
        return start_date
    
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
    return date_list[-1][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_oee_table(min_datetime=None):

    ideal_cycle_time = 4

    df = spark.sql("SELECT * FROM ManufacturingData.dbo.production_quality")

    if min_datetime:
        min_datetime_p = datetime.strptime(min_datetime, "%Y-%m-%d %H:%M")
        df = df.filter(df.timestamp >= min_datetime_p)

    df = df.withColumn(
        "date_hour",
        F.date_format(F.col("timestamp"), "yyyy-MM-dd HH:00")
    )

    # Create 10-second time bins
    binned_df = df.withColumn(
        "timebin10s",
        F.from_unixtime((F.unix_timestamp("timestamp") / 10).cast("long") * 10)
    )

    # Define window partitioned by date_hour, machine_id, site_id
    window_spec = Window.partitionBy("date_hour", "machine_id", "site_id")

    # Add production_start and production_end using window functions
    binned_df = binned_df.withColumn("production_start", F.min("timebin10s").over(window_spec))
    binned_df = binned_df.withColumn("production_end", F.max("timebin10s").over(window_spec))

    # Calculate distinct bins count per group using window
    binned_df = binned_df.withColumn(
        "Uptime_in_sec",
        F.size(F.collect_set("timebin10s").over(window_spec)) * 10
    )

    # Calculate duration
    binned_df = binned_df.withColumn(
        "duration_seconds",
        F.lit(3600)
    )

    # Calculate Availability
    binned_df = binned_df.withColumn(
        "Availability",
        F.when(F.col("duration_seconds") > 0, F.col("Uptime_in_sec") / F.col("duration_seconds"))
        .otherwise(0)
    )

    # Aggregate results
    result_df = binned_df.groupBy("machine_id", "site_id", "date_hour").agg(
        F.first("Availability").alias("Availability"),
        F.count("product_id").alias("actual_output"),
        F.sum("cycle_time_seconds").alias("sum_cycle_time_in_seconds"),
        F.count("product_id").alias("Total_Products"),
        F.sum("first_pass_yield").alias("Good_Products")
    )

    result_df = result_df.filter(
        F.col("machine_id").isNotNull() & F.col("site_id").isNotNull()
    )

    result_df = result_df.withColumn(
        "Quality",
        F.when(F.col("Total_Products") > 0,  F.col("Good_Products")/F.col("Total_Products"))
        .otherwise(0)
    )

    result_df = result_df.withColumn(
        "ideal_cycle_time",
        F.lit(ideal_cycle_time)
    )
    result_df = result_df.withColumn(
        "ideal_output",
        F.col("sum_cycle_time_in_seconds")/F.col("ideal_cycle_time")
    )

    result_df = result_df.withColumn(
        "Performance",
        F.when(F.col("Ideal_Output") > 0,  F.col("Total_Products")/F.col("Ideal_Output"))
        .otherwise(0)
    )

    result_df = result_df.withColumn(
        "OEE",
        F.col("Performance")*F.col("Quality")*F.col("Availability")
    )

    if min_datetime:
        try:
            no_of_deleted = spark.sql(f"DELETE FROM ManufacturingData.dbo.oee WHERE date_hour >= '{min_datetime}'").collect()[0][0]
        except:
            print("Table does not exist")
            no_of_deleted = 0

    else: 
        try:
            no_of_deleted = spark.sql(f"DELETE FROM ManufacturingData.dbo.oee").collect()[0][0]
        except:
            print("Table does not exist")
            no_of_deleted = 0


    print(f"Number of deleted rows: {no_of_deleted}")
     
    result_df.write.format("delta").mode("append").saveAsTable("dbo.OEE")
    no_of_inserted = result_df.count()
    print(f"Number of inserted rows: {no_of_inserted}")
    latest_datehour_df = spark.sql(f"SELECT MAX(date_hour) FROM ManufacturingData.dbo.oee")
    latest_datehour_oee = latest_datehour_df.collect()[0][0]
    return latest_datehour_oee



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#update_oee_table()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

update_date_table_ = False
update_oee_table_ = False
max_date = datetime(2025, 1, 1, 0, 0, 0)
min_datetime = datetime(2030, 1, 1)

try: 
    latest_datehour_df = spark.sql(f"SELECT MAX(date_hour) FROM ManufacturingData.dbo.oee")
    latest_datehour_oee = latest_datehour_df.collect()[0][0]
    latest_datehour_oee_tp = datetime.strptime(latest_datehour_oee, "%Y-%m-%d %H:%M")
except:
    print("Table does not exist")
    latest_datehour_oee_tp = max_date 

while True:
    

    for job in jobs:
        timestamps  = stream_yo(job["source_table"], job["target_schema"], job["target_table"])
        if timestamps["count"]:
            print(job["target_table"], timestamps)
            if timestamps["max"] > max_date:
                update_date_table_ = True
            if job["target_table"] == "production_quality" and (timestamps["min"] > latest_datehour_oee_tp):
                update_oee_table_ = True
                min_datetime = min([min_datetime, timestamps["min"]])

    if update_date_table_:
        print("Updating date table")
        max_date = update_date_table()
        print("Success")
        update_date_table_ = False
    
    if update_oee_table_:
        print("Updating OEE table")
        min_datetime_ = min_datetime.replace(minute=0, second=0, microsecond=0)
        min_datetime_ = datetime.strftime(min_datetime, "%Y-%m-%d %H:%M")
        latest_datehour_oee = update_oee_table(min_datetime_)
        latest_datehour_oee_tp = datetime.strptime(latest_datehour_oee, "%Y-%m-%d %H:%M")
        print("Success")
        update_oee_table_ = False
        
    sleep(10)

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
