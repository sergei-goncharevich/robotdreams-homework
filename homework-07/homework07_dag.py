from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from functions.etl_library import db_export
from functions.etl_library import copy_to_silver

hdfs_url = 'http://127.0.0.1:50070/'
hdfs_user = 'user'

dshop_config = {
    'bronzeOutputDir': '/bronze/dshop',
    'silverOutputDir': '/silver',
    'pgConnId': 'dshop',
    'hdfs_url': hdfs_url,
    'hdfs_user': hdfs_user
}

table_list = ('aisles', 'clients', 'departments', 'orders', 'products')

db_dag = DAG(
    'homework_07_dshop',
    start_date=datetime(2021, 8, 13),
    schedule_interval='@daily'
)


def table_export_group(table_name):
    return PythonOperator(
        dag=db_dag,
        task_id=f'db_dshop_{table_name}',
        provide_context=True,
        python_callable=db_export,
        params=dshop_config,
        op_kwargs={'table_name': table_name}
    )


def copy_to_silver_group(table_name):
    return PythonOperator(
        dag=db_dag,
        task_id=f'copy_to_silver_{table_name}',
        provide_context=True,
        python_callable=copy_to_silver,
        params=dshop_config,
        op_kwargs={'table_name': table_name}
    )


dummy_db_export_start = DummyOperator(
    task_id='db_export_start',
    dag=db_dag
)


dummy_db_export_end = DummyOperator(
    task_id='db_export_end',
    dag=db_dag
)


dummy_silver_copying_end = DummyOperator(
    task_id='dummy_silver_copying_end',
    dag=db_dag
)


for table in table_list:
    dummy_db_export_start >> table_export_group(table) >> dummy_db_export_end >> copy_to_silver_group(table) >> dummy_silver_copying_end
