import airflow
import pathlib
import pendulum
import urllib
from urllib import request
from pathlib import Path
from airflow import DAG 
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

dag = DAG(
    dag_id = 'wiki_stocksense_test',
    schedule='@hourly',
    start_date = pendulum.now().subtract(days=3), 
    template_searchpath = '/tmp/wiki'
)

'''
pull_wiki_data = BashOperator(
    task_id = 'pull_wiki_data_task',
    bash_command =
    "mkdir -p /tmp/wiki && "
    "curl -o /tmp/wiki/wiki.gz " 
    "https://dumps.wikimedia.org/other/pageviews/"
    "{{macros.ds_format(ds.subtract(hours=5),'%Y-%m-%d','%Y')}}/"
    "{{macros.ds_format(ds.subtract(hours=5),'%Y-%m-%d','%Y-%m')}}/"
    "pageviews-{{macros.ds_format(ds.subtract(hours=5),'%Y-%m-%d','%Y%m%d')}}-""{{macros.ds_format(ds.subtract(hours=5),'%Y-%m-%d','%H')}}0000.gz",
    dag=dag)
'''

def _pull_data(**kwargs):
    year,month,day,hour,*_ = kwargs['logical_date'].subtract(hours=5).timetuple()
    url = (f"https://dumps.wikimedia.org/other/pageviews/"
           f"{year:04d}/{year:04d}-{month:02d}/"
           f"pageviews-{year:04d}{month:02d}{day:02d}-{hour:02d}0000.gz")
    request.urlretrieve(url, kwargs['output_path'])

pull_wiki_data = PythonOperator(
    task_id = 'pull_wiki_data_task',
    python_callable=_pull_data,
    op_kwargs={'output_path': '/tmp/wiki/wiki.gz'},
    dag=dag
)


unzip_wiki_data = BashOperator(
    task_id = 'unzip_wiki_data_task',
    bash_command = 'gunzip --force /tmp/wiki/wiki.gz',
    dag=dag)


def _get_data(**kwargs):
    print(kwargs['input_list'],dict.fromkeys(kwargs['input_list'],0))
    result = dict.fromkeys(kwargs['input_list'],0)
    print(result)
    with open(kwargs['input_path'],'r') as f:
        for line in f:
            domain,page,view_counts,*_ = line.split(' ')
            if domain == 'en' and page in result:
                result[page] += int(view_counts)
    with open('/tmp/wiki/wiki_stock_sense_postgres.sql','w') as f:
        for page, count in result.items():
            f.write(f"INSERT INTO page_views (report_datetime,page_name,view_count) VALUES ('{kwargs['logical_date'].subtract(hours=5).start_of('hour')}','{page}', {count});\n")

get_page_data = PythonOperator(
    task_id='get_data_task',
    python_callable=_get_data,
    op_kwargs={'input_path': '/tmp/wiki/wiki',
               'input_list' : {'Google','Amazon','Microsoft','Apple','Meta','Netflix','Tesla'},
               },
    dag=dag)

del_postgres_data = SQLExecuteQueryOperator(
    task_id='del_postgres_data_task',
    sql="DELETE FROM page_views WHERE report_datetime = '{{logical_date.subtract(hours=5).start_of('hour')}}';",
    conn_id='local-postgres',
    dag=dag
)

load_postgres_data = SQLExecuteQueryOperator(
    task_id='load_postgres_data_task',
    sql='wiki_stock_sense_postgres.sql',
    conn_id='local-postgres',
    dag=dag)

pull_wiki_data >> unzip_wiki_data >> get_page_data >> del_postgres_data >> load_postgres_data