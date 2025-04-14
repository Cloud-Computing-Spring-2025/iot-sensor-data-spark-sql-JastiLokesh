from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark Session (if not already initialized)
spark = SparkSession.builder \
    .appName("IoT Sensor Data Analysis") \
    .getOrCreate()

# Load the data if not already loaded
sensor_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("sensor_data.csv")

# Convert timestamp string to proper timestamp
sensor_df_with_ts = sensor_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Extract hour of day
sensor_df_with_hour = sensor_df_with_ts.withColumn("hour_of_day", hour(col("timestamp")))

# Create pivot table with location as rows, hour_of_day as columns
pivot_df = sensor_df_with_hour.groupBy("location") \
    .pivot("hour_of_day", [i for i in range(24)]) \
    .agg(round(avg("temperature"), 1).alias("avg_temp"))

# Collect the data to find the maximum temperature
pivot_data = pivot_df.collect()
max_temp = 0
max_location = ""
max_hour = 0

for row in pivot_data:
    location = row["location"]
    for hour in range(24):
        # Handle potential NULLs
        if row[str(hour)] is not None:
            temp = float(row[str(hour)])
            if temp > max_temp:
                max_temp = temp
                max_location = location
                max_hour = hour

# Save pivot table to CSV
pivot_df.write.mode("overwrite").option("header", "true").csv("/workspaces/iot-sensor-data-spark-sql-JastiLokesh/output/task5_output.csv")
