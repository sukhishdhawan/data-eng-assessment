from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.upload_to_gs import upload_to_gcs
from utils.gs_to_bigquery import load_parquet_to_bigquery


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
        load_bigquery_task = PythonOperator(
            task_id=f'load_bigquery_{f}',
            python_callable=load_parquet_to_bigquery,
            op_kwargs={
                'project_id': 'trusty-drive-434711-g9', 
                'dataset_id': 'nyc_taxi',
                'table_id': f,
                'exec_date' : '{{ds}}'
            }
        ) 
        upload_task >> load_bigquery_task