from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from ..functions.pg_bronze import download_from_postgres, tables_from_postgres


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
    start_date=datetime(2021,5,25,1,0),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id = 'dummy_1',
    dag = dag
)


def Dynamic_Function(variable):
    return PythonOperator(
        task_id=f'pg_dump_task_{variable}',
        dag=dag,
        python_callable=download_from_postgres,
        op_kwargs = {"tbl_name": variable}
    )

dummy2 = DummyOperator(
    task_id = 'dummy_2',
    dag = dag
)

for variable in tables_from_postgres():
    value, = variable
    dummy1 >> Dynamic_Function(value) >> dummy2
