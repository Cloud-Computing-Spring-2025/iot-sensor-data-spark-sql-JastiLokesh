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
sensor_df.createOrReplaceTempView("sensor_readings")

# Filter rows where temperature is below 18 or above 30
out_of_range_df = sensor_df.filter((col("temperature") < 18) | (col("temperature") > 30))
in_range_df = sensor_df.filter((col("temperature") >= 18) & (col("temperature") <= 30))

out_of_range_count = out_of_range_df.count()
in_range_count = in_range_df.count()

# Group by location and compute average temperature and humidity
location_agg_df = sensor_df.groupBy("location") \
    .agg(
        round(avg("temperature"), 1).alias("avg_temperature"),
        round(avg("humidity"), 1).alias("avg_humidity")
    ) \
    .orderBy(desc("avg_temperature"))  # Sort by average temperature (descending)

# Save the aggregated results to CSV
location_agg_df.write.mode("overwrite").option("header", "true").csv("/workspaces/iot-sensor-data-spark-sql-JastiLokesh/output/task2_output.csv")