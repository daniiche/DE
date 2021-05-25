from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

from ..functions.data_silver import main

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'silver_dag',
    description='copy data to silver level dag',
    schedule_interval='@daily',
    start_date=datetime(2021,5,25,1,0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='to_silver',
    dag=dag,
    python_callable=main
)