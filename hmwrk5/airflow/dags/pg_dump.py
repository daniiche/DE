import os
import psycopg2
import yaml
from hdfs import InsecureClient
import logging
from datetime import datetime

def load_config():
    config_path = 'pg_config.yaml'
    with open(os.path.join(os.getcwd(), config_path), mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        logging.info('Load config ok')
        return config

# точно так же можно скопировать в бд
def download_from_postgres():
    config = load_config()['pg_config']

    client = InsecureClient('http://127.0.0.1:50070/', user='user')
    # прередать все параметры из словаря, две здездочки раскрыли словарь
    with psycopg2.connect(**config) as pg_connection:
        cursor = pg_connection.cursor()
        # так гораздо бытсрее - не надо итерировать и минимум питона - сразу в файл
        # нет промежуточного хранения в оперативной памяти
        # для постгреса нативно цсв и особо нет передачи в бинарном формате, поэтому в цсв

        cursor.execute("""SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';""")
        for tuple in cursor.fetchall():
            value, = tuple

            path = f'/data/{value}'

            with client.write('path'+'/users.csv') as csv:
                cursor.copy_expert(f'COPY {value} TO STDOUT WITH HEADER CSV', csv)

        cursor.close()


if __name__ == '__main__':

    logging.basicConfig(filename=f'TT_parse_{datetime.now()}.log',
                        filemode='w',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Start loading')
    total_start = datetime.now()
    download_from_postgres()
    total_finish = datetime.now()
    logging.info('Total elapsed: ')
    logging.info(total_finish - total_start)
