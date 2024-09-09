# data-eng-assessment
We will be doing this assessment in GCP free tier.

Google Doc Link - https://docs.google.com/document/d/1aX0vTrG03R84NLSzyaQMzQtkMgKgRGPkdeCWAuPqF38

Data Source -  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

1. Prerequisites -

* Setting up Airflow (Google Composer)
![Screenshot 2024-09-10 at 3 24 48 AM](https://github.com/user-attachments/assets/835ad434-71a3-45a7-b9a4-378a76dd59a5)


* Dataproc cluster
![Screenshot 2024-09-10 at 3 27 07 AM](https://github.com/user-attachments/assets/79912bd3-4715-473c-81eb-b1c44876a320)

2. Setting Up Airflow Dag - 

* Now, upload all the files and subfolders in directory ``dags`` to the airflow dags folder so that the composer can pickup our dag.
![Screenshot 2024-09-10 at 3 33 10 AM](https://github.com/user-attachments/assets/efebb274-d9bd-4e18-928c-b5b431c2608e)

* After the dag is picked up successfully, it should be visible something like this - 
![Screenshot 2024-09-10 at 3 36 12 AM](https://github.com/user-attachments/assets/b1319c7f-f383-41ad-8864-43c91d11df4b)


* Once you trigger a dag for a particular date say ``2023-06-05`` , it will pickup the files from source for June, 2023 month and load into the bigquery final table ``trip_data_consolidated``

3. Analysis on dataset -

* You can do analysis on the dataset similar to the one done in notebook file ``/analysis/Exploratory_Data_Analysis.ipynb`` in jupyter notebook.
* Jupyter notebook can be accessed in the dataproc cluster
  ![Screenshot 2024-09-10 at 3 42 46 AM](https://github.com/user-attachments/assets/46b3a97f-bf19-4bdb-ac91-af65d60ca290)
