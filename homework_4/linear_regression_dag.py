from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io
import pendulum
from datetime import timedelta
from sklearn.linear_model import LinearRegression

# Constants
S3_BUCKET  = "benson-gillespie-liu-mwaa"
CSV_PREFIX = "weather_data"
S3_OUTPUT  = "predictions"

# Numeric columns in csvs
NUM_COLS = ['temperature','dewpoint','windSpeed','barometricPressure','visibility','relativeHumidity','heatIndex']

# Define feature cols from numerical columns
FEAT_COLS = [c for c in NUM_COLS if c != 'temperature']

# Output col names
TEMP_KEYS = ['thirty_temp_pred','hour_temp_pred','hour_thirty_temp_pred',
    'two_hour_temp_pred','two_hour_thirty_temp_pred','three_hour_temp_pred','three_hour_thirty_temp_pred',
    'four_hour_temp_pred','four_hour_thirty_temp_pred',
    'five_hour_temp_pred','five_hour_thirty_temp_pred','six_hour_temp_pred','six_hour_thirty_temp_pred',
    'seven_hour_temp_pred','seven_hour_thirty_temp_pred',
    'eight_hour_temp_pred'
]

# Use same default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

# Same dag function as before
dag = DAG(
    dag_id="linear_regression_predictions",
    default_args=default_args,
    description="HW4: train & predict 20h/40h linear regression on weather_data",
    # Use timedelta to set off every 20 hours
    schedule_interval=timedelta(hours=20),
    catchup=False,
    max_active_runs=1,
    tags=['DE300'],
)

# Define main function
def predict_temperature():
    hook = S3Hook(aws_conn_id='aws_default')

    # Find all files under weather_data bucket, for some reason i had to add a slash
    keys = hook.list_keys(bucket_name=S3_BUCKET, prefix=CSV_PREFIX+'/')
    dfs =[]
    for key in keys:
        if key.lower().endswith('.csv'):
            content = hook.read_key(key=key, bucket_name=S3_BUCKET)
            # Use io.stringio similar to last file
            df = pd.read_csv(io.StringIO(content), parse_dates=['timestamp'])
            dfs.append(df)

    # Concatenate and clean files
    combined = pd.concat(dfs, ignore_index=True)
    combined['timestamp'] =pd.to_datetime(combined['timestamp'], utc=True)
    combined[NUM_COLS] = combined[NUM_COLS].apply(pd.to_numeric, errors='coerce')

    # Look through the last 20 and 40 hours orf data
    for hours in (20, 40):
        # Stop looking at information when we are before our time delta
        cutoff =combined['timestamp'].max() - timedelta(hours=hours)
        window= combined[combined['timestamp'] >= cutoff]
        results = []

        # For each station, create a new x/y using groupby
        for station, sub in window.groupby('station'):
            # Only sample the numeric cols, was getting a bunch of bad inputs with all cols
            # the below function also front fills and back fills any missing data
            sub =(sub.set_index('timestamp')[NUM_COLS].resample('30T').mean().ffill().bfill().reset_index())

            # Create lag and target cols
            sub['temp_t'] = sub['temperature']
            sub['temp_t-1'] = sub['temperature'].shift(1)
            for i in range(1, 17):
                sub[f'temp_t+{i}'] =sub['temperature'].shift(-i)

            # Define our needed columns, drop any na values 
            needed= ['temp_t','temp_t-1'] + FEAT_COLS + [f'temp_t+{i}' for i in range(1,17)]
            sub2 = sub.dropna(subset=needed)

            # Define x and y dataframes
            X =sub2[['temp_t','temp_t-1'] + FEAT_COLS]
            y = sub2[[f'temp_t+{i}' for i in range(1,17)]]

            # Predict for all future times
            model = LinearRegression().fit(X, y)
            preds = model.predict(X.iloc[[-1]])[0]

            # Update our results list
            row = {'station': station}
            row.update(dict(zip(TEMP_KEYS, preds)))
            results.append(row)

        # Create a dataframe of results, turn into a csv
        df_out = pd.DataFrame(results)
        fname= f"predicted_temperatures_{hours}_hours.csv"
        local=f"/tmp/{fname}"
        df_out.to_csv(local, index=False)

        # Upload to S3 under predictions/
        hook.load_file(filename=local,key=f"{S3_OUTPUT}/{fname}",bucket_name=S3_BUCKET,replace=True)

# Run function using PythonOperator
fetch_and_upload_task = PythonOperator(
    task_id="create_linear_regression_temperature_predictions",
    python_callable=predict_temperature,
    dag=dag
)
