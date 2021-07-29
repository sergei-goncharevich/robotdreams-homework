from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from api_plugin import run
from airflow.hooks.postgres_hook import PostgresHook
import os

api_config = {
    'baseUrl': 'https://robot-dreams-de-api.herokuapp.com',
    'outputDir': '/home/user/data',
    'connId': 'dshop',
    'auth': {'endpoint': '/auth',
             'username': 'rd_dreams',
             'password': 'djT6LasE'},
    'api': {'endpoint': '/out_of_stock'}
}

dag = DAG(
    'data_export_dag',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 10),
    schedule_interval='@daily'
)


def api_export(**kwargs):
    run(kwargs['params'], kwargs['ds'])


api_operator = PythonOperator(
    dag=dag,
    task_id='api_export',
    provide_context=True,
    python_callable=api_export,
    params=api_config
)


db_dag = DAG(
    'db_export_dag',
    start_date=datetime(2021, 7, 26),
    end_date=datetime(2021, 7, 26),
    schedule_interval='@daily',
    params=api_config
)


def db_export(**kwargs):
    config = kwargs['params']
    hook = PostgresHook(postgres_conn_id=config['connId'])
    with hook.get_cursor() as cursor:
        cursor.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\'')
        for rec in cursor.fetchall():
            table_name = rec[0]
            output_dir = os.path.join(config['outputDir'], 'tables')
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, f'{table_name}.csv'), 'w') as output_file:
                cursor.copy_expert(f'COPY {table_name} TO STDOUT WITH HEADER CSV', output_file)


db_operator = PythonOperator(
    dag=db_dag,
    task_id='db_export',
    provide_context=True,
    python_callable=db_export
)

