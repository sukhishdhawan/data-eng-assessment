from google.cloud import bigquery

def load_parquet_to_bigquery(project_id, dataset_id, table_id, exec_date):
    gcs_uri = f'gs://nyc-taxi-dhawan/NYC_TAXI/{table_id}_{exec_date[:7]}.parquet'
    client = bigquery.Client(project=project_id)
    #Delete Records before inserting
    delete_query = f"""
    DELETE FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE file_month = PARSE_DATE("%Y-%m-%d", "{exec_date[:7]}-01")
    """
    delete_job = client.query(delete_query)
    delete_job.result()
    print(f"Deleted records for month {exec_date[:7]}")
    #Load Data
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,  
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  
    )
    load_job = client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=table_ref,
        job_config=job_config
    )
    load_job.result()
    
    # Check the job status
    if load_job.error_result:
        raise Exception(f"Failed to load data: {load_job.error_result}")
    
    print(f"Loaded {load_job.output_rows} rows into {table_ref}.")
    #Update file_month after insertion
    update_query = f"""
    UPDATE `{project_id}.{dataset_id}.{table_id}`
    SET file_month = PARSE_DATE("%Y-%m-%d", "{exec_date[:7]}-01")
    WHERE file_month IS NULL
    """
    update_job = client.query(update_query)
    update_job.result()
    print(f"Updated records for month {exec_date[:7]}")