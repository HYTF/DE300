# Import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pandas as pd
import pendulum
import time
from datetime import datetime

# Define constants
WEATHER_STATIONS = ["KORD", "KENW", "KMDW", "KPNT"]
BASE_URL = "https://api.weather.gov/stations/{station}/observations/latest"
S3_BUCKET = "benson-gillespie-liu-mwaa"
S3_PREFIX = "weather_data"

# Use the same default_args as the lab
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1
}

# Define our DAG() function, with a small change in schedule_interval
dag = DAG(
    dag_id="collect_weather_data_every_2_hours",
    default_args=default_args,
    description="HW4 dag for Benson, Gillespie, and Liu",

    # Main change from lab, this schedules the DAG to run every second hour
    # starting from the beginning of the day
    schedule_interval="0 */2 * * *",

    tags=['DE300']
)

# Define primary function for our DAG, which runs every two hours
def collect_and_upload_weather_data():


    # Given code to extract data
    collected_data = {}
    for station in WEATHER_STATIONS:
        url = BASE_URL.format(station=station)
        response = requests.get(url)
        if response.status_code == 200:
            collected_data[station] = response.json()
        else:
            collected_data[station] = {"error": "Failed to fetch data"}
        time.sleep(2)

    # Turn data into rows so we can make a usable df
    rows=[]
    time_of_collection = datetime.utcnow().isoformat()
    for station, full_json_file in collected_data.items():
        properties=full_json_file.get('properties', {})
        rows.append({
            'station': station,
            'timeOfCollection': time_of_collection,
            'timestamp': properties.get('timestamp'),
            'temperature': properties.get("temperature", {}).get("value"),
            'dewpoint': properties.get("dewpoint", {}).get("value"),
            'windSpeed': properties.get("windSpeed", {}).get("value"),
            'barometricPressure': properties.get("barometricPressure", {}).get("value"),
            'visibility': properties.get("visibility", {}).get("value"),
            'precipitationLastHour': properties.get("precipitationLastHour", {}).get("value"),
            'relativeHumidity': properties.get("relativeHumidity", {}).get("value"),
            'heatIndex': properties.get("heatIndex", {}).get("value"),
        })
        
    # Put collected data into a dataframe
    df = pd.DataFrame(rows)

    # Name file using utcnow() so we can track .csvs over time
    filename = f"weather_data_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"

    # Save dataframe as a csv file
    filepath = f"/tmp/{filename}"
    df.to_csv(filepath, index=False)

    # Upload csv to our s3 bucket
    s3 = S3Hook(aws_conn_id='aws_default')
    s3.load_file(filename=filepath, key=f"{S3_PREFIX}/{filename}", bucket_name=S3_BUCKET, replace=False)

# Create PythonOperator() task
fetch_and_upload_task = PythonOperator(
    task_id="fetch_and_upload_weather_data",
    python_callable=collect_and_upload_weather_data,
    dag=dag,
)
