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

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType, IntegerType, DoubleType
import random
from datetime import datetime, timedelta

# Configuration
DOWNTIME_PERCENTAGE = 0.30  # 30% of days will have downtime anomalies
QUALITY_ANOMALY_PERCENTAGE = 0.30  # 30% of days will have FPY anomalies
CYCLE_TIME_ANOMALY_PERCENTAGE = 0.30  # 30% of days will have cycle time anomalies

FPY_FAILURE_PROBABILITY = 0.30  # 50% chance of FPY being false during anomaly window
CYCLE_TIME_MULTIPLIER = 1.8  # Increase cycle time by 80%
ANOMALY_WINDOW_DURATION = 4  # Hours duration of each anomaly window

# Load the original data
df = spark.sql("SELECT * FROM ManufacturingData.dbo.production_quality")

# Extract distinct dates for anomaly selection
df = df.withColumn("date_only", F.to_date("timestamp"))
df = df.withColumn("hour_of_day", F.hour("timestamp"))

distinct_dates = df.select("date_only").distinct().collect()
all_dates = [row["date_only"] for row in distinct_dates]

# Randomly select dates for each type of anomaly (independent selections)
random.seed(42)  # For reproducibility
num_dates = len(all_dates)

downtime_dates = set(random.sample(all_dates, int(num_dates * DOWNTIME_PERCENTAGE)))
quality_dates = set(random.sample(all_dates, int(num_dates * QUALITY_ANOMALY_PERCENTAGE)))
cycle_time_dates = set(random.sample(all_dates, int(num_dates * CYCLE_TIME_ANOMALY_PERCENTAGE)))

# Generate random start hours for each anomaly date (varying times per day)
# Hours range from 6 AM to 20 PM (leaving room for 2-hour window)
def generate_random_hour():
    return random.randint(6, 22 - ANOMALY_WINDOW_DURATION)

# Create dictionaries mapping date -> random start hour for each anomaly type
downtime_hours = {str(d): generate_random_hour() for d in downtime_dates}
quality_hours = {str(d): generate_random_hour() for d in quality_dates}
cycle_time_hours = {str(d): generate_random_hour() for d in cycle_time_dates}

# Convert to lists for broadcasting
downtime_dates_list = list(downtime_hours.keys())
quality_dates_list = list(quality_hours.keys())
cycle_time_dates_list = list(cycle_time_hours.keys())

print(f"Total dates: {num_dates}")
print(f"\nDowntime anomaly dates ({len(downtime_dates_list)}):")
for d in list(downtime_hours.items())[:5]:
    print(f"  {d[0]}: {d[1]}:00 - {d[1] + ANOMALY_WINDOW_DURATION}:00")

print(f"\nQuality anomaly dates ({len(quality_dates_list)}):")
for d in list(quality_hours.items())[:5]:
    print(f"  {d[0]}: {d[1]}:00 - {d[1] + ANOMALY_WINDOW_DURATION}:00")

print(f"\nCycle time anomaly dates ({len(cycle_time_dates_list)}):")
for d in list(cycle_time_hours.items())[:5]:
    print(f"  {d[0]}: {d[1]}:00 - {d[1] + ANOMALY_WINDOW_DURATION}:00")

# Broadcast the dictionaries for efficient lookup
downtime_hours_bc = spark.sparkContext.broadcast(downtime_hours)
quality_hours_bc = spark.sparkContext.broadcast(quality_hours)
cycle_time_hours_bc = spark.sparkContext.broadcast(cycle_time_hours)

# UDF to check if record is in downtime window
@F.udf(BooleanType())
def is_in_downtime_window(date_str, hour):
    if date_str in downtime_hours_bc.value:
        start_hour = downtime_hours_bc.value[date_str]
        return start_hour <= hour < start_hour + ANOMALY_WINDOW_DURATION
    return False

# UDF to check if record is in quality anomaly window
@F.udf(BooleanType())
def is_in_quality_window(date_str, hour):
    if date_str in quality_hours_bc.value:
        start_hour = quality_hours_bc.value[date_str]
        return start_hour <= hour < start_hour + ANOMALY_WINDOW_DURATION
    return False

# UDF to check if record is in cycle time anomaly window
@F.udf(BooleanType())
def is_in_cycle_time_window(date_str, hour):
    if date_str in cycle_time_hours_bc.value:
        start_hour = cycle_time_hours_bc.value[date_str]
        return start_hour <= hour < start_hour + ANOMALY_WINDOW_DURATION
    return False

# Add helper column for date string
df = df.withColumn("date_str", F.date_format("date_only", "yyyy-MM-dd"))

# --- ANOMALY 1: Create Downtime (delete records) ---
df = df.withColumn(
    "is_downtime_anomaly",
    is_in_downtime_window(F.col("date_str"), F.col("hour_of_day"))
)

# Remove downtime records (simulates machine being down)
df_with_downtime = df.filter(~F.col("is_downtime_anomaly"))

# --- ANOMALY 2: Quality Issues (set first_pass_yield to false with 50% probability) ---
df_with_quality = df_with_downtime.withColumn(
    "is_quality_anomaly",
    is_in_quality_window(F.col("date_str"), F.col("hour_of_day"))
)

# Add random value for probabilistic FPY failure
df_with_quality = df_with_quality.withColumn("random_val", F.rand(seed=123))

# Use 0 instead of False to match INT data type
df_with_quality = df_with_quality.withColumn(
    "first_pass_yield",
    F.when(
        (F.col("is_quality_anomaly")) & (F.col("random_val") < FPY_FAILURE_PROBABILITY),
        F.lit(0)  # Use 0 instead of False for INT column
    ).otherwise(F.col("first_pass_yield"))
)

# --- ANOMALY 3: Performance Issues (increase cycle time) ---
df_with_anomalies = df_with_quality.withColumn(
    "is_cycle_time_anomaly",
    is_in_cycle_time_window(F.col("date_str"), F.col("hour_of_day"))
)

df_with_anomalies = df_with_anomalies.withColumn(
    "cycle_time_seconds",
    F.when(
        F.col("is_cycle_time_anomaly"),
        F.col("cycle_time_seconds") * CYCLE_TIME_MULTIPLIER
    ).otherwise(F.col("cycle_time_seconds"))
)

# Clean up helper columns
df_final = df_with_anomalies.drop(
    "date_only", "hour_of_day", "date_str", 
    "is_downtime_anomaly", "is_quality_anomaly", "is_cycle_time_anomaly", "random_val"
)

# Show summary of changes
print("\n--- Anomaly Summary ---")
original_count = df.count()
after_downtime_count = df_with_downtime.count()
print(f"Original record count: {original_count}")
print(f"After downtime anomaly (deleted records): {after_downtime_count}")
print(f"Records deleted for downtime: {original_count - after_downtime_count}")

quality_window_count = df_with_quality.filter(F.col("is_quality_anomaly")).count()
quality_affected = df_with_quality.filter(
    (F.col("is_quality_anomaly")) & (F.col("first_pass_yield") == False)
).count()
print(f"Records in quality anomaly window: {quality_window_count}")
print(f"Records with forced FPY failure (~50%): {quality_affected}")

cycle_time_affected = df_with_anomalies.filter(F.col("is_cycle_time_anomaly")).count()
print(f"Records with increased cycle time: {cycle_time_affected}")

# Write back to table (or create a new anomaly table)
#df_final.write.mode("overwrite").saveAsTable("ManufacturingData.dbo.production_quality_with_anomalies")

# Or overwrite the original table (uncomment if needed):
#df_final.write.mode("overwrite").saveAsTable("ManufacturingData.dbo.production_quality")

print("\nAnomalies created successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.write.mode("overwrite").saveAsTable("ManufacturingData.dbo.production_quality")

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
