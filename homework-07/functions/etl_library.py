import logging
import os
from airflow.hooks.postgres_hook import PostgresHook
from hdfs import InsecureClient
from pyspark.sql import SparkSession


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
            cursor.copy_expert(f'COPY {table_name} TO STDOUT WITH HEADER CSV', output_file)
    logging.info(f'Export of table {table_name} successfully completed')


def copy_to_silver(**kwargs):
    table_name = kwargs['table_name']
    logging.info(f'Start copying {table_name} to silver')
    config = kwargs['params']
    current_date = kwargs['ds']
    spark = SparkSession \
        .builder.master('local') \
        .appName('homework-07') \
        .getOrCreate()
    df = spark.read.load(os.path.join(config['bronzeOutputDir'], table_name, current_date, table_name + '.csv')
                         , header="true"
                         , inferSchema="true"
                         , format="csv")
    # removing duplicate rows
    df = df.drop_duplicates()

    # writing result to silver
    df.write.parquet(os.path.join(config['silverOutputDir'], table_name), mode='overwrite')
    logging.info(f'Copying of table {table_name} to silver successfully completed')
