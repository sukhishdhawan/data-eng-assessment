from google.cloud import storage
import requests
import os

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