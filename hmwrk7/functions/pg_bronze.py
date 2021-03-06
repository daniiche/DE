import os
import psycopg2
import yaml
from hdfs import InsecureClient
import logging
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

#if run locally
# def load_config():
#     config_path = 'pg_config.yaml'
#     with open(os.path.join(os.getcwd(), config_path), mode='r') as yaml_file:
#         config = yaml.safe_load(yaml_file)
#         logging.info('Load config ok')
#         return config
#if run from airflow
def load_config():
    connection = BaseHook.get_connection('oltp_postgres')

    config = {
            'host': connection.host
            , 'port': connection.port
            , 'database': connection.schema
            , 'user': connection.login
            , 'password': connection.password
    }
    logging.info('Load config ok')
    return config

# точно так же можно скопировать в бд
def tables_from_postgres():
    config = load_config()
    # прередать все параметры из словаря, две здездочки раскрыли словарь
    with psycopg2.connect(**config) as pg_connection:
        cursor = pg_connection.cursor()
        # так гораздо бытсрее - не надо итерировать и минимум питона - сразу в файл
        # нет промежуточного хранения в оперативной памяти
        # для постгреса нативно цсв и особо нет передачи в бинарном формате, поэтому в цсв

        cursor.execute("""SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';""")
        tbl_lst = cursor.fetchall()
        logging.info(tbl_lst)
        return tbl_lst

def download_from_postgres(tbl_name):
    config = load_config()
    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    with psycopg2.connect(**config) as pg_connection:
        cursor = pg_connection.cursor()

        path = f'/bronze/{datetime.now().strftime("%Y-%m-%d")}'
        with client.write(path+f'/{tbl_name}.csv') as csv:
            cursor.copy_expert(f'COPY {tbl_name} TO STDOUT WITH HEADER CSV', csv)


if __name__ == '__main__':

    logging.basicConfig(
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Start loading')
    total_start = datetime.now()
    tbls = tables_from_postgres()
    for tbl in tbls:
        tbl, = tbl
        download_from_postgres(tbl)
    total_finish = datetime.now()
    logging.info('Total elapsed: ')
    logging.info(total_finish - total_start)
