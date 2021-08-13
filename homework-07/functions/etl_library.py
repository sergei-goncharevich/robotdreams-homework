import logging
from airflow.hooks.postgres_hook import PostgresHook
from hdfs import InsecureClient


def db_export(**kwargs):
    table_name = kwargs['table_name']
    logging.info(f'Starting export of table {table_name}')
    config = kwargs['params']
    current_date = kwargs['ds']
    hook = PostgresHook(postgres_conn_id=config['connId'])
    client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])
    with hook.get_cursor() as cursor:
        output_dir = config['outputDir'] + f'/{table_name}/{current_date}'
        client.makedirs(output_dir)
        with client.write(hdfs_path=f'{output_dir}/{table_name}.csv',
                          overwrite=True
                          ) as output_file:
            cursor.copy_expert(f'COPY {table_name} TO STDOUT WITH HEADER CSV', output_file)
    logging.info(f'Export of table {table_name} successfully completed')
