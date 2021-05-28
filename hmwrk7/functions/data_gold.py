from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
import datetime
from airflow.hooks.base_hook import BaseHook


def load_config():
    connection = BaseHook.get_connection('oltp_greenplum')

    config = {
        'gp_url' : f"jdbc:postgresql://{connection.host}:{connection.port}/{connection.schema}"
        ,'gp_properties' : {"user": connection.login, "password": connection.password}
    }
    logging.info('Load config ok')
    return config

def main():
    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.20.jar')\
        .master('local')\
        .appName("lesson")\
        .getOrCreate()

    logging.info('Loading from silver')
    webhdfs = f'webhdfs://127.0.0.1:50070'
    current_date = datetime.date.today()

    logging.info('Loading aisles')
    path = f'/silver/{current_date}/aisles'
    aisles = spark.read.parquet(webhdfs+path)

    logging.info('Loading clients')
    path = f'/silver/{current_date}/clients'
    clients = spark.read.parquet(webhdfs+path)

    logging.info('Loading departments')
    path = f'/silver/{current_date}/departments'
    departments = spark.read.parquet(webhdfs+path)

    logging.info('Loading location_areas')
    path = f'/silver/{current_date}/location_areas'
    location_areas = spark.read.parquet(webhdfs+path)

    logging.info('Loading orders')
    path = f'/silver/{current_date}/orders'
    orders = spark.read.parquet(webhdfs+path)

    logging.info('Loading products')
    path = f'/silver/{current_date}/products'
    products = spark.read.parquet(webhdfs+path)

    logging.info('Loading store_types')
    path = f'/silver/{current_date}/store_types'
    store_types = spark.read.parquet(webhdfs+path)

    logging.info('Loading stores')
    path = f'/silver/{current_date}/stores'
    stores = spark.read.parquet(webhdfs+path)

    out_of_stock = None
    try:
        logging.info('Loading out stocks')
        path = f'/silver/{current_date}/out_of_stock'
        out_of_stock = spark.read.parquet(webhdfs+path)
    except:
        logging.error(f'No out stocks for today')
    logging.info('Parquets loaded')


    stores = stores.join(store_types, 'store_type_id')\
        .select(stores.store_id
                ,stores.location_area_id
                ,store_types.type)

    products = stores.join(aisles, 'aisle_id') \
        .join(departments, 'department_id') \
        .select(products['*']
                , aisles.aisle
                , departments.department)

    columns = ['date', 'year', 'month', 'weekday', 'day']
    time = spark.createDataFrame([(
        current_date
        ,current_date.year
        ,current_date.month
        ,current_date.weekday()
        ,current_date.day)], columns)

    conf = load_config()
    time.write.jdbc(conf['gp_url']
                      , table='dim_time'
                      , properties=conf['gp_properties']
                      , mode='overwrite')

    stores.write.jdbc(conf['gp_url']
                     , table='dim_stores'
                     , properties=conf['gp_properties']
                     , mode='overwrite')

    products.write.jdbc(conf['gp_url']
                      , table='dim_products'
                      , properties=conf['gp_properties']
                      , mode='overwrite')

    location_areas.write.jdbc(conf['gp_url']
                      , table='dim_slocation_areas'
                      , properties=conf['gp_properties']
                      , mode='overwrite')

    clients.write.jdbc(conf['gp_url']
                      , table='dim_clients'
                      , properties=conf['gp_properties']
                      , mode='overwrite')

    out_of_stock.write.jdbc(conf['gp_url']
                      , table='fact_oos'
                      , properties=conf['gp_properties']
                      , mode='overwrite')

    orders.write.jdbc(conf['gp_url']
                      , table='dim_orders'
                      , properties=conf['gp_properties']
                      , mode='overwrite')

if __name__ == '__main__':
    main()