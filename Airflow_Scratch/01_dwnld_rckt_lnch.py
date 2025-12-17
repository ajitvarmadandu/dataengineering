import json
import pathlib
import requests
import airflow
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from requests import exceptions as request_exceptions

dag = DAG(
    dag_id='download_rocket_launch_data',
    start_date=pendulum.now().subtract(days=3),
    schedule=None
)

download_rckt_lnch = BashOperator(
    task_id='download_rocket_launch_data_task',
    bash_command="curl -L -o /tmp/launches.json 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)

def get_pics():
    pathlib.Path('/tmp/images').mkdir(parents=True, exist_ok=True)

    with open('/tmp/launches.json') as f:
        data = json.load(f)
    
    pics = []
    for launch in data['results']:
        if launch['image']:
            pics.append(launch['image'])
    
    for pic in pics:
            try:
                 response = requests.get(pic)
                 file_name = pic.split("/")[-1]
                 tgt = '/tmp/images/' + file_name
                 with open(tgt, 'wb') as img_file:
                     img_file.write(response.content)
                
                 print(f"Downloaded image: {file_name} to {tgt}")
            
            except request_exceptions.MissingSchema:
                 print(f"Invalid URL {pic}, skipping...")

            except request_exceptions.ConnectionError:
                 print(f"Connection error while downloading {pic}, skipping...")


get_pics = PythonOperator(
    task_id='download_rocket_launch_images_task',  
    python_callable=get_pics,
    dag=dag
)

notify = BashOperator(
    task_id='notify_task',
    bash_command='echo "Rocket launch data and images $(ls /tmp/images | wc -l) downloaded successfully!"',
    dag=dag
)

download_rckt_lnch >> get_pics >> notify