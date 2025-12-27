import airflow
import spotipy
import boto3
import io
import pendulum
import pandas as pd
import json
from airflow.models import Variable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from spotipy.oauth2 import SpotifyClientCredentials
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 12, 1, tz="UTC")
}

def _fetch_data_from_spotify(**kwargs):
    client_id = Variable.get('spotify_client_id')
    client_secret = Variable.get('spotify_client_secret')

    # Use SpotifyClientCredentials instead of SpotifyOAuth
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    playlist_link = kwargs['playist_link']
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]

    spotify_data = sp.playlist_tracks(playlist_URI)

    file_name = "spotify_raw_" + str(pendulum.now()) + ".json"
    kwargs['ti'].xcom_push(key='file_name', value=file_name)

    kwargs['ti'].xcom_push(key='body', value=json.dumps(spotify_data))


dag = DAG(
    dag_id = 'spotify_data_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)

fetch_spotify_data = PythonOperator(
    task_id='fetch_data_from_spotify',
    python_callable=_fetch_data_from_spotify,
    op_kwargs={'playist_link': "https://open.spotify.com/playlist/1YUVG3tWJL8bYRpNiRFd29?si=U_MN2Iy4QcuKEtl03u2XnQ"},
    dag=dag)

push_raw_data_to_s3 = S3CreateObjectOperator(
    task_id='push_raw_data_to_s3',
    aws_conn_id='aws-s3-bucket',
    s3_bucket='airbnb-proj',
    s3_key='spotify/raw/to_be_processed/{{ ti.xcom_pull(task_ids="fetch_data_from_spotify", key="file_name") }}',
    data='{{ ti.xcom_pull(task_ids="fetch_data_from_spotify", key="body") }}',
    replace=True,
    dag=dag)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

start >> fetch_spotify_data >> push_raw_data_to_s3 >> end

