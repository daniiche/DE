from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
from datetime import datetime

def main():
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.20.jar')\
        .master('local')\
        .appName("lesson")\
        .getOrCreate()

    logging.info('Loading from silver')
    webhdfs = f'webhdfs://127.0.0.1:50070'

    logging.info('Loading aisles')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/aisles'
    aisles_df = spark.read.parquet(webhdfs+path)

    logging.info('Loading clients')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/clients'
    clients_df = spark.read.parquet(webhdfs+path)

    logging.info('Loading departments')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/departments'
    departments_df = spark.read.parquet(webhdfs+path)

    logging.info('Loading location_areas')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/location_areas'
    location_areas_df = spark.read.parquet(webhdfs+path)

    logging.info('Loading orders')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/orders'
    orders_df = spark.read.parquet(webhdfs+path)

    logging.info('Loading products')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/products'
    products_df = spark.read.parquet(webhdfs+path)

    logging.info('Loading store_types')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/store_types'
    store_types_df = spark.read.parquet(webhdfs+path)

    logging.info('Loading stores')
    path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/stores'
    stores_df = spark.read.parquet(webhdfs+path)

    try:
        logging.info('Loading out stocks')
        path = f'/silver/{datetime.now().strftime("%Y-%m-%d")}/out_of_stock'
        out_of_stock_df = spark.read.parquet(webhdfs+path)
    except:
        logging.error(f'No out stocks for today')
    logging.info('Parquets loaded')

if __name__ == '__main__':
    main()