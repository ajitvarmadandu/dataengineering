import airflow
import pathlib
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pathlib import Path
import pandas as pd
import csv
import numpy as np
import psycopg2
import requests
from custom_ops.postgres_aws_s3_operator import PostgresToS3Operator

with DAG(dag_id = 'airbnb_s3_copy',
         schedule = None,
         start_date = pendulum.now().subtract(days=3),
         template_searchpath = '/tmp/airbnb') as dag:

    def _download_airbnb_data(**kwargs):
        url_template = 'https://data.insideairbnb.com/united-states/ny/new-york-city/{listing_date}/visualisations/listings.csv'
        for listing_date in kwargs['listing_dates']:
            url = url_template.format(listing_date=listing_date)
            response = requests.get(url)
            if response.status_code == 200:
                with open(f"/tmp/airbnb/listings_{listing_date}.csv", 'wb') as f:
                    f.write(response.content)
            else:
                print(f"Data for {listing_date} not found at {url}")

    download_airbnb_data = PythonOperator(
        task_id = 'download_airbnb_data_task',
        python_callable=_download_airbnb_data,
        op_kwargs={'listing_dates': ['2025-09-01', '2025-10-01', '2025-11-01','2025-12-04']},
        dag=dag
    )

    def _preprocess_airbnb_data(**kwargs):
        for listing_date in kwargs['listing_dates']:
            input_path = f"/tmp/airbnb/listings_{listing_date}.csv"
            output_path = f"/tmp/airbnb/processed_listings_{listing_date}.csv"
            df = pd.read_csv(input_path)
            df.fillna('', inplace=True)
            df[['id','name','host_id','host_name']].to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)

    preprocess_airbnb_data = PythonOperator(
        task_id = 'preprocess_airbnb_data_task',
        python_callable=_preprocess_airbnb_data,
        op_kwargs={'listing_dates': ['2025-09-01', '2025-10-01', '2025-11-01','2025-12-04']},
        dag=dag
    )

    def _load_csv_to_postgres(**kwargs):
        conn = psycopg2.connect(
            dbname='scratch',
            user='postgres',
            host='host.docker.internal',
            password='Dvnr@3003',
            port='5432')

        cursor = conn.cursor()
        for listing_date in kwargs['listing_dates']:
            input_path = f"/tmp/airbnb/processed_listings_{listing_date}.csv"
            with open(input_path, 'r') as f:
                next(f) # Skip header row
                cursor.copy_expert("COPY airbnb_newyork_listings FROM stdin WITH CSV HEADER QUOTE '\"' ", f)
        conn.commit()
        cursor.close()
        conn.close()


    load_csv_to_postgres = PythonOperator(
        task_id = 'load_csv_to_postgres_task',
        python_callable=_load_csv_to_postgres,
        op_kwargs={'listing_dates': ['2025-09-01', '2025-10-01', '2025-11-01','2025-12-04']},
        dag=dag
    )

    transfer_postgres_to_s3 = PostgresToS3Operator(
        task_id='transfer_postgres_to_s3_task',
        postgres_conn_id='local-postgres',
        s3_conn_id='aws-s3-bucket',
        s3_bucket='airbnb-proj',
        s3_key='airbnb/airbnb_newyork_listings.csv',
        sql_query='SELECT * FROM airbnb_newyork_listings;',
        dag=dag
    )

    download_airbnb_data >> preprocess_airbnb_data >> load_csv_to_postgres >> transfer_postgres_to_s3