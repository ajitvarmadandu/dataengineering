import json
import pathlib
import requests
import airflow
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
#from airflow.operators.postgres_operator import PostgresOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from requests import exceptions as request_exceptions

POSTGRES_CONN = 'local-postgres'

dag = DAG(
    dag_id = 'postgres_insert_test',
    start_date = pendulum.now().subtract(days=3),
    schedule=None
)

create_table = SQLExecuteQueryOperator(
    task_id='create_table_task',
    conn_id=POSTGRES_CONN,
    sql="""
    CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, name VARCHAR(50));
    """,    
    dag=dag
)


create_table