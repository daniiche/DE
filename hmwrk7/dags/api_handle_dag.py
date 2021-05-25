from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

from ..functions.api_bronze import auth

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'api_handle_dag',
    description='api handle etl currency dags',
    schedule_interval='@daily',
    start_date=datetime(2021,5,25,1,0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='currency_function',
    dag=dag,
    python_callable=auth
)