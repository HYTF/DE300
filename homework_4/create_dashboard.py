# Import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import pendulum
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
import io

# Define bucket naming conventions
S3_BUCKET = "benson-gillespie-liu-mwaa"
S3_PREFIX = "output_graphs"

# Use the same default_args as the lab
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1
}

# Define our DAG() function to run daily
dag = DAG(
    dag_id="create_dashboard",
    default_args=default_args,
    description="Create dashboard dag, hw4 for Benson, Gillespie, and Liu",
    schedule="@daily",
    tags=['DE300']
)

# Create a function that creates and uploads a dashboard
def create_and_upload_dashboard():

    # Use s3Hook so we can load files from s3 bucket
    s3 = S3Hook(aws_conn_id='aws_default')

    # Determine date of the files we want to look at in this format "year_month_day"
    # Subtract a day so we find the previous day in utc
    target_date=(datetime.now(timezone.utc)-timedelta(days=1)).strftime('%m_%d_%Y')

    # List all files under the weather_data folder
    keys=s3.list_keys(bucket_name=S3_BUCKET, prefix='weather_data')

    # Create string that we are looking for
    prefix_string=f'date_{target_date}'

    # Create a list called "day_keys" that finds all keys from the previous day
    day_keys=[]
    for key in keys:
        if prefix_string in key:
            day_keys.append(key)

    # Read through each key, use s3.read_key to find the csv content, load into a dataframe
    dfs=[]
    for key in day_keys:
        csv_content = s3.read_key(key=key, bucket_name=S3_BUCKET)

        # Using io, open the csv content so we aren't reading it as a string, but as a csv
        df=pd.read_csv(io.StringIO(csv_content))
        dfs.append(df)

    # Combine dfs together into one concatenated df
    combined_df=pd.concat(dfs, ignore_index=True)

    # Make sure timestamp is saved as datetime
    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])

    # Initialize metrics and ylabels
    metrics=['temperature','visibility','relativeHumidity']
    ylabels={
        'temperature': 'Temperature (Degrees C)',
        'visibility': 'Visibility (m)',
        'relativeHumidity': 'Relative Humidity (%)'
    }

    # Create a pivot dataframe along the timestamp so we can track metrics over time
    for metric in metrics:
        pivot_df=combined_df.pivot(index='timestamp',columns='station', values=metric)

        # Create title and filename so we can upload
        ylabel=ylabels[metric]
        title=f'{ylabel} on {target_date}'
        filename=f'{metric}_{target_date}.png'

        # Create a figure and plot
        fig, plot=plt.subplots(figsize=(12,8))

        # Loop through each station and plot on the same figure
        for station in pivot_df.columns:
            # Drop na timeseries values
            dropped_series=pivot_df[station].dropna()

            # Plot current station
            plot.plot(dropped_series.index, dropped_series.values,
                      marker='o', linestyle='-', label=station
            )

        # Plot all stations on one axis
        plot.set_title(title)
        plot.set_xlabel('Timestamp (In UTC)')
        plot.set_ylabel(ylabel)
        plot.legend(title='Station')

        # Save as a png, send to s3 bucket
        plot.get_figure().savefig(filename)
        plt.close(plot.get_figure())
        s3.load_file(filename=filename, key=f'output_graphs/{filename}', bucket_name=S3_BUCKET, replace=True)

# Create PythonOperator() task
fetch_and_upload_task = PythonOperator(
    task_id="create_daily_dashboard",
    python_callable=create_and_upload_dashboard,
    dag=dag,
)
