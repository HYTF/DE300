# DE300 Weather Data Pipeline

This project is a weather data engineering pipeline implemented using Apache Airflow. It collects real-time observations from NOAA weather stations every two hours, generates daily dashboards, and trains a temperature prediction model once enough data is collected.

## Source Code

- download_data.py: Collects weather data from NOAA API every 2 hours and saves CSVs to S3
- create_dashboard.py: Runs daily and generates 3 graphs (temperature, visibility, relative humidity) for the past day
- linear_regression_dag.py: Runs twice per full DAG execution once after 20 hours and again after 40 hours of data. Trains a linear regression model for each weather station using lag features and predicts temperature for the next 8 hours. Results are saved to S3 as CSV.



## Setup & Execution

### Environment Requirements

- AWS MWAA
- Python
- Python packages (already installed on MWAA):
  - requests, pandas, scikit-learn, matplotlib, io, boto3, s3fs
- Our S3 bucket with the following structure:
  - weather_data/ — where raw weather CSVs are stored
  - output_graphs/ — where daily dashboard images are stored
  - predictions/ — where model prediction CSVs are uploaded

### Upload DAGs to MWAA

Upload the following Python files to your MWAA DAGs folder:
- download_data.py
- create_dashboard.py
- linear_regression_dag.py


## Outputs

### Weather Data (S3: weather_data/)
- Collected as CSV files every 2 hours
- Each file contains one row per station with:
  - temperature, dewpoint, windSpeed, barometricPressure, visibility, precipitationLastHour, relativeHumidity, heatIndex

### 2. Dashboard Graphs (S3: output_graphs/)
- One .png file per metric (temperature, visibility, relativeHumidity) per day
- Visualizes time series data from all 4 stations on a single plot

### 3. Model Predictions (S3: predictions/)
- CSV files with model predictions for the next 8 hours at 30-minute intervals
- Run at 20 and 40-hour marks
- Output columns: station, thirty_temp_pred, hour_temp_pred, hour_thirty_temp_pred, ..., eight_hour_temp_pred


## Gen AI Disclosure

- Gen AI (ChatGPT) was used in this assignment. Below are the prompts:
- Why is the precipitation column empty?
- Is it okay to impute NA values in the precipitation column to 0 since it has no rain?
- How to create PythonOperator() task?
- Please help me debug T_T
