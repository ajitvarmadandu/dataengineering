import airflow
import pendulum
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


BATCH_TS = pendulum.now().to_iso8601_string()
GCP_PROJECT_ID = 'data-engineering-482616'
BQ_DATASET = 'airflow_scratch'
BQ_TABLE = 'crypto_data'

def _fetch_api_data(**kwargs):
    url=kwargs['url']
    response = requests.get(url,params=kwargs.get('params', {}))
    data = response.json()
    file_name = "crypto_raw_" + BATCH_TS + ".json"
    v_raw_file_name = kwargs['ti'].xcom_push(key='v_raw_file_name', value=file_name)
    with open(file_name, 'w') as f:
        json.dump(data, f)

def _process_data(**kwargs):
    with open(kwargs['ti'].xcom_pull(task_ids='fetch_api_data', key='v_raw_file_name'), 'r') as f:
        data = json.load(f)
        processed_data = []
        for item in data:
            processed_data.append({'id' : item['id'], 
                                   'symbol': item['symbol'],
                                   'current_price': item['current_price'],
                                   'high_24h': item['high_24h'],
                                   'low_24h': item['low_24h'],
                                   'price_change_24h': item['price_change_24h'],
                                   'load_ts': BATCH_TS})
        file_name = "crypto_processed_" + BATCH_TS + ".csv"
        v_processed_file_name = kwargs['ti'].xcom_push(key='v_processed_file_name', value=file_name)
        df = pd.DataFrame(processed_data)
        df.to_csv(file_name, index=False)

dag= DAG(
    dag_id='crypto_data_pipeline',
    start_date=pendulum.now().subtract(days=1),
    schedule=None,
    catchup=False
)

start = EmptyOperator(task_id='start', dag=dag)

fetch_api_data = PythonOperator(
    task_id='fetch_api_data',
    python_callable=_fetch_api_data,
    op_kwargs={
        'url': 'https://api.coingecko.com/api/v3/coins/markets',
        'params': {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 10,
            'page': 1,
            'sparkline': 'false'
        }
    },
    dag=dag
)

create_gcs_bucket = GCSCreateBucketOperator(
    task_id='create_gcs_bucket',
    bucket_name='crypto-data-bucket-dandu',
    project_id='data-engineering-482616',
    gcp_conn_id='gcp-airflow-conn',
    dag=dag
)

upload_raw_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_raw_to_gcs',
    src="{{ ti.xcom_pull(task_ids='fetch_api_data', key='v_raw_file_name') }}",
    dst="raw/{{ ti.xcom_pull(task_ids='fetch_api_data', key='v_raw_file_name') }}",
    bucket='crypto-data-bucket-dandu',
    gcp_conn_id='gcp-airflow-conn',
    dag=dag
)

transform_data = PythonOperator(
    task_id='process_data',
    python_callable=_process_data,
    dag=dag)

upload_processed_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_processed_to_gcs',
    src="{{ ti.xcom_pull(task_ids='process_data', key='v_processed_file_name') }}",
    dst="processed/{{ ti.xcom_pull(task_ids='process_data', key='v_processed_file_name') }}",
    bucket='crypto-data-bucket-dandu',
    gcp_conn_id='gcp-airflow-conn',
    dag=dag
)

load_processed_bq = GCSToBigQueryOperator(
    task_id="processed_gcs_to_bigquery",
    bucket="crypto-data-bucket-dandu",
    source_objects=["processed/{{ ti.xcom_pull(task_ids='process_data', key='v_processed_file_name') }}"],
    destination_project_dataset_table=f"{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
    schema_fields=[
        {"name": "id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
        {"name": "current_price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "high_24h", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "low_24h", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "price_change_24h", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "load_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
    write_disposition="WRITE_APPEND",
    gcp_conn_id='gcp-airflow-conn',
    source_format='CSV',
    skip_leading_rows=1,
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag)

start >> fetch_api_data >> create_gcs_bucket >> [upload_raw_to_gcs, transform_data]
transform_data >> upload_processed_to_gcs >> load_processed_bq
[upload_raw_to_gcs, load_processed_bq] >> end