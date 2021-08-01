from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
import requests
from hdfs import InsecureClient

hdfs_url = 'http://127.0.0.1:50070/'
hdfs_user = 'user'

api_config = {
    'baseUrl': 'https://robot-dreams-de-api.herokuapp.com',
    'outputDir': '/bronze/api',
    'auth': {'endpoint': '/auth',
             'username': 'rd_dreams',
             'password': 'djT6LasE'},
    'api': {'endpoint': '/out_of_stock'},
    'hdfs_url': hdfs_url,
    'hdfs_user': hdfs_user
}

dshop_config = {
    'outputDir': '/bronze/dshop',
    'connId': 'dshop',
    'hdfs_url': hdfs_url,
    'hdfs_user': hdfs_user
}

dshop_bu_config = {
    'outputDir': '/bronze/dshop_bu',
    'connId': 'dshop_bu',
    'hdfs_url': hdfs_url,
    'hdfs_user': hdfs_user
}

api_dag = DAG(
    'homework_05_api_export',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 2),
    schedule_interval='@daily'
)


def api_export(**kwargs):
    config = kwargs['params']
    current_date = kwargs['ds']
    jwt = auth(config)

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


def auth(config):
    auth_config = config['auth']
    response = requests.post(url=config['baseUrl'] + auth_config['endpoint'],
                             json={'username': auth_config['username'], 'password': auth_config['password']}
                             )
    response.raise_for_status()
    return response.json()['access_token']


api_operator = PythonOperator(
    dag=api_dag,
    task_id='api_export',
    provide_context=True,
    python_callable=api_export,
    params=api_config
)


db_dag = DAG(
    'homework_05_dshop',
    start_date=datetime(2021, 7, 26),
    schedule_interval='@daily',
)


def db_export(**kwargs):
    config = kwargs['params']
    current_date = kwargs['ds']
    hook = PostgresHook(postgres_conn_id=config['connId'])
    client = InsecureClient(config['hdfs_url'], user=config['hdfs_user'])
    with hook.get_cursor() as cursor:
        cursor.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\'')
        for rec in cursor.fetchall():
            table_name = rec[0]
            output_dir = config['outputDir'] + f'/{table_name}/{current_date}'
            client.makedirs(output_dir)
            with client.write(hdfs_path=f'{output_dir}/{table_name}.csv',
                              overwrite=True
                              ) as output_file:
                cursor.copy_expert(f'COPY {table_name} TO STDOUT WITH HEADER CSV', output_file)


db_dshop = PythonOperator(
    dag=db_dag,
    task_id='db_dshop',
    provide_context=True,
    python_callable=db_export,
    params=dshop_config
)


db_dshop_bu = PythonOperator(
    dag=db_dag,
    task_id='db_dshop_bu',
    provide_context=True,
    python_callable=db_export,
    params=dshop_bu_config
)


[db_dshop, db_dshop_bu]