import os
import psycopg2
import yaml

def load_config():
    config_path = 'pg_config.yaml'
    with open(os.path.join(os.getcwd(), config_path), mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config
# точно так же можно скопировать в бд
def download_from_postgres():
    config = load_config()['pg_config']
    # прередать все параметры из словаря, две здездочки раскрыли словарь
    with psycopg2.connect(**config) as pg_connection:
        cursor = pg_connection.cursor()
        # так гораздо бытсрее - не надо итерировать и минимум питона - сразу в файл
        # нет промежуточного хранения в оперативной памяти
        # для постгреса нативно цсв и особо нет передачи в бинарном формате, поэтому в цсв

        cursor.execute("""SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';""")
        for tuple in cursor.fetchall():
            value, = tuple
            path = os.path.join(os.getcwd(), f'data/{value}')

            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                except OSError:
                    print("Creation of the directory %s failed" % path)
                else:
                    print("Successfully created the directory %s" % path)
            with open(os.path.join(path, f'{value}.csv'), 'w') as csv_file:
                 cursor.copy_expert(f'COPY {value} TO STDOUT WITH HEADER CSV', csv_file)
        cursor.close()


if __name__ == '__main__':
    download_from_postgres()
