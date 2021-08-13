from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from functions.etl_library import db_export

hdfs_url = 'http://127.0.0.1:50070/'
hdfs_user = 'user'

dshop_config = {
    'outputDir': '/bronze/dshop',
    'connId': 'dshop',
    'hdfs_url': hdfs_url,
    'hdfs_user': hdfs_user
}

table_list = ('aisles', 'clients', 'departments', 'orders', 'products')

db_dag = DAG(
    'homework_07_dshop',
    start_date=datetime(2021, 8, 12),
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


dummy_start = DummyOperator(
    task_id='db_export_start',
    dag=db_dag
)


dummy_end = DummyOperator(
    task_id='db_export_end',
    dag=db_dag
)

for table in table_list:
    dummy_start >> table_export_group(table) >> dummy_end
