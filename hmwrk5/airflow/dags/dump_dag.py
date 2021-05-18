from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pg_dump import download_from_postgres, tables_from_postgres

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'pg_dump_dag',
    description='pg dump dag',
    schedule_interval='@daily',
    start_date=datetime(2021,5,4,1,0),
    default_args=default_args
)


def Dynamic_Function(variable):
    t1 = PythonOperator(
        task_id=f'pg_dump_task_{variable}',
        dag=dag,
        python_callable=download_from_postgres,
        op_kwargs = {"tbl_name": variable}
    )
    return t1

for variable in tables_from_postgres():
    value, = variable
    task_1 = Dynamic_Function(value)