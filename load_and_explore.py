from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IoT Sensor Data Analysis") \
    .getOrCreate()

# Task 1: Load & Basic Exploration
# Load the CSV file
sensor_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("sensor_data.csv")

# Create a temporary view
sensor_df.createOrReplaceTempView("sensor_readings")

# Show the first 5 rows (without printing)
first_5_rows = sensor_df.limit(5)

# Count total records
total_records = sensor_df.count()

# Retrieve distinct locations
distinct_locations = sensor_df.select("location").distinct()

# Retrieve distinct sensor types
distinct_sensor_types = sensor_df.select("sensor_type").distinct()

# Save results to CSV
sensor_df.write.mode("overwrite").option("header", "true").csv("/workspaces/iot-sensor-data-spark-sql-JastiLokesh/output/task1_output.csv")