# IoT Sensor Data Analysis with PySpark

This repository contains a set of PySpark scripts for analyzing IoT sensor data. The project is structured to process sensor readings from different locations and sensor types, performing various data analysis tasks.

## Project Structure

- `sensor_data.csv`: Input CSV file containing sensor readings with columns for sensor_id, timestamp, temperature, humidity, location, and sensor_type.
- Python scripts:
  - `load_and_explore.py`: Load and explore the dataset (Task 1)
  - `filter_and_aggregate.py`: Filter and aggregate data (Task 2)
  - `time_based_analysis.py`: Analyze data by time (Task 3)
  - `sensor_ranking.py`: Rank sensors by temperature (Task 4)
  - `pivot_analysis.py`: Create and analyze pivot tables (Task 5)
- Output files:
  - `task1_output.csv` to `task5_output.csv`: Results of each analysis task

## Environment Requirements

- Python 3.6+
- Apache Spark 3.0+
- PySpark

## Task Descriptions

### Task 1: Load & Basic Exploration
- Loads the CSV data
- Creates a temporary view
- Performs basic data exploration
- Outputs the full dataset to `task1_output.csv`

### Task 2: Filtering & Simple Aggregations
- Filters temperature readings outside the 18-30°C range
- Calculates average temperature and humidity by location
- Outputs the aggregated results to `task2_output.csv`

### Task 3: Time-Based Analysis
- Converts string timestamps to proper timestamp format
- Groups data by hour of day
- Calculates average temperature for each hour
- Identifies the hottest hour of the day
- Outputs hourly analysis to `task3_output.csv`

### Task 4: Window Function
- Calculates average temperature for each sensor
- Ranks sensors by their average temperature
- Identifies the top 5 sensors with highest average temperatures
- Outputs the ranked sensors to `task4_output.csv`

### Task 5: Pivot & Interpretation
- Creates a pivot table with location as rows and hours as columns
- Fills cells with average temperature values
- Identifies the location and hour with the highest average temperature
- Outputs the pivot table to `task5_output.csv`

## Running the Scripts

Each script can be run independently using spark-submit:

```bash
spark-submit load_and_explore.py
spark-submit filter_and_aggregate.py
spark-submit time_based_analysis.py
spark-submit sensor_ranking.py
spark-submit pivot_analysis.py
```


## Analysis Findings

The analysis reveals several patterns in the sensor data:

1. Temperature variations exist across different locations, with some buildings/floors consistently recording higher temperatures.
2. Certain hours of the day show peak temperatures across all locations.
3. The top 5 sensors by average temperature are identified, providing insights into potential hotspots or sensor calibration issues.
4. The pivot table in Task 5 enables a comprehensive view of temperature patterns by location and time.

## Future Improvements

- Add data validation and cleaning steps
- Implement anomaly detection for sensor readings
- Explore correlations between temperature and humidity
- Visualize the results using matplotlib or other libraries