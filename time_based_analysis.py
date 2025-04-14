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

# Create a new temp view with proper timestamp
sensor_df_with_ts.createOrReplaceTempView("sensor_readings_with_ts")

# Extract hour of day and group by hour
hourly_temp_df = sensor_df_with_ts.withColumn("hour_of_day", hour(col("timestamp"))) \
    .groupBy("hour_of_day") \
    .agg(round(avg("temperature"), 1).alias("avg_temp")) \
    .orderBy("hour_of_day")

# Find the hour with highest average temperature
max_temp_hour = hourly_temp_df.orderBy(desc("avg_temp")).first()

# Save results to CSV
hourly_temp_df.write.mode("overwrite").option("header", "true").csv("/workspaces/iot-sensor-data-spark-sql-JastiLokesh/output/task3_output.csv")