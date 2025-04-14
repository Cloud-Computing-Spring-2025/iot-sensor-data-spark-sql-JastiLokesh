from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session (if not already initialized)
spark = SparkSession.builder \
    .appName("IoT Sensor Data Analysis") \
    .getOrCreate()

# Load the data if not already loaded
sensor_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("sensor_data.csv")

# Compute each sensor's average temperature
sensor_avg_temp = sensor_df.groupBy("sensor_id") \
    .agg(round(avg("temperature"), 1).alias("avg_temp"))

# Define window specification for ranking
window_spec = Window.orderBy(desc("avg_temp"))

# Add rank column based on average temperature (descending)
ranked_sensors = sensor_avg_temp.withColumn("rank_temp", dense_rank().over(window_spec))

# Show top 5 sensors by temperature
top_5_sensors = ranked_sensors.filter(col("rank_temp") <= 5)

# Save top 5 results to CSV
top_5_sensors.write.mode("overwrite").option("header", "true").csv("/workspaces/iot-sensor-data-spark-sql-JastiLokesh/output/task4_output.csv")