from pyspark.sql import SparkSession
from pg_bronze import tables_from_postgres

from datetime import datetime
import logging

logging.basicConfig(
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    current_date = datetime.now().strftime("%Y-%m-%d")

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.20.jar')\
        .master('local')\
        .appName("lesson")\
        .getOrCreate()

    webhdfs = f'webhdfs://127.0.0.1:50070'

    for tbl in tables_from_postgres():
        tbl, = tbl
        logging.info(f'Reading {tbl}')
        load_path = f'/bronze/{datetime.now().strftime("%Y-%m-%d")}/{tbl}.csv'
        df = spark.read.load(webhdfs+load_path
                                        , header="true"
                                        , inferSchema="true"
                                        , format="csv")
        df = df \
            .distinct()

        save_path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/{tbl}'
        df.write \
                .parquet(webhdfs+save_path, mode='overwrite')
        logging.info(f'{tbl} saved to silver')

    try:
        logging.info(f'Reading out stocks')
        load_path = f'/bronze/{datetime.now().strftime("%Y-%m-%d")}/out_of_stock.json'
        df = spark.read.load(webhdfs+load_path
                                        , inferSchema="true"
                                        , format="json")

        df = df \
            .distinct()

        save_path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/out_of_stock'
        df.write \
            .parquet(webhdfs + save_path, mode='overwrite')


        logging.info(f'Out stocks saved to silver')
    except:
        logging.error(f'No out stocks for today')

if __name__ == '__main__':
    main()