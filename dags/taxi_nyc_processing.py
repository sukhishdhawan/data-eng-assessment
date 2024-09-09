from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
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
project_id = 'trusty-drive-434711-g9'

# Define the PySpark job configuration
pyspark_job_config = {
    'reference': {
        'project_id': project_id,
    },
    'placement': {
        'cluster_name': 'cluster-main',
    },
    'pyspark_job': {
        'main_python_file_uri': 'gs://us-central1-main-env-17ba29ee-bucket/dags/utils/nyc_taxi_transform.py',
        'args': ['{{ ds }}'],
    },
}

# Initialize the DAG
with DAG(
    'taxi_nyc_processing',
    default_args=default_args,
    description='taxi_nyc_processing',
    schedule_interval=None,  # Run on demand
    start_date=datetime(2023, 6, 1),
    catchup=True,
) as dag:
    load_bigquery_tasks = []

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
                'project_id': project_id, 
                'dataset_id': 'nyc_taxi',
                'table_id': f,
                'exec_date' : '{{ds}}'
            }
        ) 

        load_bigquery_tasks.append(load_bigquery_task)
        upload_task >> load_bigquery_task

    taxi_transform_task = DataprocSubmitJobOperator(
        task_id='taxi_transform_task',
        job=pyspark_job_config,
        region='us-central1',
        project_id=project_id,
        dag=dag,
    )   
    
    load_bigquery_tasks >> taxi_transform_task
