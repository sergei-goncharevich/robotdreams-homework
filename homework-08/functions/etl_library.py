import logging
import os
import requests
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowFailException
from hdfs import InsecureClient
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime


def db_export(**kwargs):
    table_name = kwargs['table_name']
    logging.info(f'Starting export of table {table_name}')
    config = kwargs['params']
    current_date = kwargs['ds']
    hook = PostgresHook(postgres_conn_id=config['pgConnId'])
    client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])
    with hook.get_cursor() as cursor:
        output_dir = config['bronzeOutputDir'] + f'/{table_name}/{current_date}'
        client.makedirs(output_dir)
        with client.write(hdfs_path=f'{output_dir}/{table_name}.csv',
                          overwrite=True
                          ) as output_file:
            cursor.copy_expert(f'COPY {table_name} TO STDOUT WITH DELIMITER \'{config["csv_delimiter"]}\' HEADER CSV'\
                               , output_file)
    logging.info(f'Export of table {table_name} successfully completed')


def fact_data_export(**kwargs):
    current_date = kwargs['ds']
    table_name = 'orders'
    sql_query = f'select o.order_id, o.product_id, o.client_id, o.store_id, o.quantity from orders o where o.order_date = \'{current_date}\''
    logging.info(f'Starting export of table {table_name} on {current_date}')
    config = kwargs['params']
    hook = PostgresHook(postgres_conn_id=config['pgConnId'])
    client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])
    with hook.get_cursor() as cursor:
        output_dir = config['bronzeOutputDir'] + f'/{table_name}/{current_date}'
        client.makedirs(output_dir)
        with client.write(hdfs_path=f'{output_dir}/{table_name}.csv',
                          overwrite=True
                          ) as output_file:
            cursor.copy_expert(f'COPY ({sql_query}) TO STDOUT WITH DELIMITER \'{config["csv_delimiter"]}\' HEADER CSV'\
                               , output_file)
    logging.info(f'Export of table {table_name} successfully completed')


def copy_to_silver(**kwargs):
    table_name = kwargs['table_name']
    partitions_count = kwargs['partitions_count']
    logging.info(f'Start copying {table_name} to silver')
    config = kwargs['params']
    current_date = kwargs['ds']
    spark = SparkSession \
        .builder.master('local') \
        .appName('homework-08') \
        .getOrCreate()
    df = spark.read.load(os.path.join(config['bronzeOutputDir'], table_name, current_date, table_name + '.csv')
                         , header="true"
                         , inferSchema="true"
                         , format="csv"
                         , delimiter=config['csv_delimiter']
                         )
    # removing duplicate rows
    df = df.drop_duplicates()

    # writing result to silver
    df.coalesce(partitions_count).write.parquet(os.path.join(config['silverOutputDir'], table_name), mode='overwrite')
    logging.info(f'Copying of table {table_name} to silver successfully completed')


def copy_to_gold(**kwargs):
    table_name = kwargs['table_name']
    logging.info(f'Start copying {table_name} to gold')
    config = kwargs['params']
    spark = SparkSession \
        .builder.master('local') \
        .appName('homework-08') \
        .config('spark.driver.extraClassPath', config['path_to_driver']) \
        .getOrCreate()
    df = spark.read.parquet(os.path.join(config['silverOutputDir'], table_name))

    # writing result to gold
    gp_conn = BaseHook.get_connection(config['gpConnId'])
    gp_properties = {"user": gp_conn.login
                     , "password": gp_conn.password
                     , "truncate": "true"
                     }
    df.write.jdbc(f'jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}'
                  , table=f'dim_{table_name}'
                  , properties=gp_properties
                  , mode='overwrite'
                  )

    logging.info(f'Copying of table {table_name} to gold successfully completed')


def orders_to_gold(**kwargs):
    table_name = 'orders'
    logging.info(f'Start copying {table_name} to gold')
    config = kwargs['params']
    current_date = kwargs['ds']

    prepare_for_fact_insert(f'fact_{table_name}', current_date, config['gpConnId'])

    spark = SparkSession \
        .builder.master('local') \
        .appName('homework-08') \
        .config('spark.driver.extraClassPath', config['path_to_driver']) \
        .getOrCreate()

    orders_df = spark.read.parquet(os.path.join(config['silverOutputDir'], table_name))

    dt = datetime.strptime(current_date, '%Y-%m-%d').date()
    df = orders_df.groupBy('product_id', 'client_id', 'store_id') \
        .agg(F.sum('quantity').alias('quantity')) \
        .select(F.lit(dt).alias('report_date'), 'product_id', 'client_id', 'store_id', 'quantity')

    # writing result to gold
    gp_conn = BaseHook.get_connection(config['gpConnId'])
    gp_properties = {"user": gp_conn.login
                     , "password": gp_conn.password
                     , "numPartitions": f'{config["fact_partitions_count"]}'
                     }

    df.write.jdbc(f'jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}'
                  , table=f'fact_{table_name}'
                  , properties=gp_properties
                  , mode='append'
                  )

    logging.info(f'Copying of table {table_name} to gold successfully completed')


def prepare_for_fact_insert(table_name, current_date, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute('select count(*) from dim_dates dd where report_date = %s', (current_date,))
            res = cursor.fetchone()
            if res[0] == 0:
                dt = datetime.strptime(current_date, '%Y-%m-%d')
                is_holiday = dt.weekday() == 5 or dt.weekday() == 6
                cursor.execute('insert into dim_dates values (%s, %s, %s)', (current_date, 1, is_holiday))
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(f"delete from {table_name} where report_date=%s", (current_date,))


def api_export(**kwargs):
    config = kwargs['params']
    current_date = kwargs['ds']
    logging.info(f'Starting api export for {current_date}')
    jwt = api_auth(config)

    url = config['baseUrl'] + config['api']['endpoint']
    headers = {'Authorization': 'JWT ' + jwt}
    response = requests.get(url=url, headers=headers, json={'date': f'{current_date}'})
    if response.status_code == 200:
        client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])
        output_dir = config['outputDir'] + f'/out-of-stock/{current_date}'
        client.makedirs(output_dir)
        with client.write(hdfs_path=f'{output_dir}/out-of-stock.json',
                          overwrite=True,
                          encoding='utf-8') as output_file:
            output_file.write(str(response.json()))
    else:
        if response.status_code == 404:
            message = response.json()['message']
            logging.error(f'Api export for {current_date} failed with message: {message}')
            raise AirflowFailException(response.json()['message'])
        else:
            logging.error(f'Api export for {current_date} failed with http code {response.status_code}')
    logging.info(f'Api export for {current_date} successfully completed')


def api_auth(config):
    auth_config = config['auth']
    response = requests.post(url=config['baseUrl'] + auth_config['endpoint'],
                             json={'username': auth_config['username'], 'password': auth_config['password']}
                             )
    response.raise_for_status()
    return response.json()['access_token']


def api_to_silver(**kwargs):
    logging.info(f'Start copying out_of_stock to silver')
    config = kwargs['params']
    current_date = kwargs['ds']
    partitions_count = config['partitions_count']
    spark = SparkSession \
        .builder.master('local') \
        .appName('homework-08') \
        .getOrCreate()
    df = spark.read.json(f"{config['outputDir']}/out-of-stock/{current_date}/out-of-stock.json")

    # removing duplicate rows
    df = df.drop_duplicates()

    # writing result to silver
    df.coalesce(partitions_count).write.parquet(os.path.join(config['silverOutputDir'], 'oos'),
                                                mode='overwrite')

    logging.info(f'Copying of out_of_stock data to silver successfully completed')


def api_to_gold(**kwargs):
    table_name = 'oos'
    logging.info(f'Start copying {table_name} to gold')
    config = kwargs['params']
    current_date = kwargs['ds']

    prepare_for_fact_insert(f'fact_{table_name}', current_date, config['gpConnId'])

    spark = SparkSession \
        .builder.master('local') \
        .appName('homework-08') \
        .config('spark.driver.extraClassPath', config['path_to_driver']) \
        .getOrCreate()

    oos_df = spark.read.parquet(os.path.join(config['silverOutputDir'], table_name))

    dt = datetime.strptime(current_date, '%Y-%m-%d').date()
    df = oos_df.select(F.lit(dt).alias('report_date'), 'product_id')

    # writing result to gold
    gp_conn = BaseHook.get_connection(config['gpConnId'])
    gp_properties = {"user": gp_conn.login
                     , "password": gp_conn.password
                     , "numPartitions": f'{config["partitions_count"]}'
                     }

    df.write.jdbc(f'jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}'
                  , table=f'fact_{table_name}'
                  , properties=gp_properties
                  , mode='append'
                  )

    logging.info(f'Copying of table {table_name} to gold successfully completed')
