import json
import pathlib
import requests
import airflow
import pendulum
import pandas as pd
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
#from airflow.operators.postgres_operator import PostgresOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from requests import exceptions as request_exceptions

dag = DAG(
    dag_id = 'event_schedule_test',
    start_date = pendulum.now().subtract(days=30),
    schedule=None
)

pull_events = BashOperator(
    task_id = 'pull_events_task',
    bash_command = 
    "mkdir -p /tmp/events && "
    "curl -o /tmp/events/{{ macros.ds_add(ds, -1) }}_events.json http://events_api:5000/events?"
    "start_date={{ macros.ds_add(ds, -1) }}&"
    "end_date={{ds}}",
    dag=dag
)

def _process_events(ip,op):
    Path(op).parent.mkdir(exist_ok=True)

    data = pd.read_json(ip)
    stats=data.groupby(['date','user']).size().reset_index()

    stats.to_csv(op, index=False)

process_events = PythonOperator(
    task_id='process_events_task',
    python_callable=_process_events,
    op_kwargs={'ip': '/tmp/events/{{ macros.ds_add(ds, -1) }}_events.json', 'op': '/tmp/stats/{{ macros.ds_add(ds, -1) }}_stats.csv'},
    dag=dag
)

pull_events >> process_events