import logging
import os
import requests
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.hdfs_hook import HDFSHook
from airflow.exceptions import AirflowFailException
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
        res_data = []
        for item in response.json():
            res_data.append(item['product_id'])
        with client.write(hdfs_path=f'{output_dir}/out-of-stock.json',
                          overwrite=True,
                          encoding='utf-8') as output_file:
            output_file.write(str(res_data))
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
