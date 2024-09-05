from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage
import requests
import os


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

file_list = ['fhvhv_tripdata', 'fhv_tripdata', 'green_tripdata', 'yellow_tripdata']

# Initialize the DAG
with DAG(
    'taxi_nyc_processing',
    default_args=default_args,
    description='taxi_nyc_processing',
    schedule_interval=None,  # Run on demand
    start_date=datetime(2023, 6, 1),
    catchup=True,
) as dag:
    
    def download_file(url, local_filename):
        response = requests.get(url)
        response.raise_for_status()
        with open(local_filename, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully: {local_filename}")

    def upload_to_gcs(bucket_name, source_file_name, exec_date):
            download_file(f'https://d37ci6vzurychx.cloudfront.net/trip-data/{source_file_name}_{exec_date[:7]}.parquet', f'{source_file_name}_{exec_date[:7]}.parquet')
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(f'NYC_TAXI/{source_file_name}_{exec_date[:7]}.parquet')
            blob.upload_from_filename(f'{source_file_name}_{exec_date[:7]}.parquet')
            print(f"File {source_file_name}_{exec_date[:7]}.parquet uploaded to {f'NYC_TAXI/{source_file_name}_{exec_date[:7]}.parquet'}.")
            os.remove(f'{source_file_name}_{exec_date[:7]}.parquet')

    for f in file_list:
        upload_task = PythonOperator(
            task_id=f'upload_to_gcs_{f}',
            python_callable=upload_to_gcs,
            op_kwargs={
                'bucket_name': 'nyc-taxi-dhawan', 
                'source_file_name': f,
                'exec_date' : '{{ds}}'
            }
        )