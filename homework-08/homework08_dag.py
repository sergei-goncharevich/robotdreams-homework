from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from functions.etl_library import db_export
from functions.etl_library import copy_to_silver
from functions.etl_library import api_export
from functions.etl_library import copy_to_gold
from functions.etl_library import fact_data_export
from functions.etl_library import orders_to_gold
from functions.etl_library import api_to_silver
from functions.etl_library import api_to_gold

hdfs_url = 'http://127.0.0.1:50070/'
hdfs_user = 'user'
dim_partitions_count = 1
fact_partitions_count = 10

dshop_config = {
    'bronzeOutputDir': '/bronze/dshop_bu',
    'silverOutputDir': '/silver',
    'pgConnId': 'dshop_bu',
    'gpConnId': 'dwh',
    'hdfs_url': hdfs_url,
    'hdfs_user': hdfs_user,
    'path_to_driver': '/home/user/drivers/postgresql-42.2.23.jar',
    'csv_delimiter': '|',
    'fact_partitions_count': fact_partitions_count
}

api_config = {
    'baseUrl': 'https://robot-dreams-de-api.herokuapp.com',
    'outputDir': '/bronze/api',
    'auth': {'endpoint': '/auth',
             'username': 'rd_dreams',
             'password': 'djT6LasE'},
    'api': {'endpoint': '/out_of_stock'},
    'hdfs_url': hdfs_url,
    'hdfs_user': hdfs_user,
    'silverOutputDir': '/silver',
    'gpConnId': 'dwh',
    'path_to_driver': '/home/user/drivers/postgresql-42.2.23.jar',
    'partitions_count': fact_partitions_count
}

table_list = ('departments', 'aisles', 'products', 'location_areas', 'clients', 'store_types', 'stores')


db_dag = DAG(
    'homework_08_dshop',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 1),
    schedule_interval='@daily'
)


api_dag = DAG(
    'homework_08_api_export',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 1),
    schedule_interval='@daily'
)


api_operator = PythonOperator(
    dag=api_dag,
    task_id='api_export',
    provide_context=True,
    python_callable=api_export,
    params=api_config
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


def copy_to_silver_group(table_name, partitions_count):
    return PythonOperator(
        dag=db_dag,
        task_id=f'copy_to_silver_{table_name}',
        provide_context=True,
        python_callable=copy_to_silver,
        params=dshop_config,
        op_kwargs={'table_name': table_name
                   , 'partitions_count': partitions_count
                   }
    )


def copy_to_gold_group(table_name):
    return PythonOperator(
        dag=db_dag,
        task_id=f'gold_dim_{table_name}',
        provide_context=True,
        python_callable=copy_to_gold,
        params=dshop_config,
        op_kwargs={'table_name': table_name}
    )


facts_export = PythonOperator(
        dag=db_dag,
        task_id=f'db_dshop_orders',
        provide_context=True,
        python_callable=fact_data_export,
        params=dshop_config
)


orders_to_gold_operator = PythonOperator(
        dag=db_dag,
        task_id=f'gold_fact_orders',
        provide_context=True,
        python_callable=orders_to_gold,
        params=dshop_config
)


oos_to_silver_operator = PythonOperator(
        dag=api_dag,
        task_id='copy_to_silver_oos',
        provide_context=True,
        python_callable=api_to_silver,
        params=api_config
)


oos_to_gold_operator = PythonOperator(
        dag=api_dag,
        task_id='gold_fact_oos',
        provide_context=True,
        python_callable=api_to_gold,
        params=api_config
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


dummy_gold_copying_end = DummyOperator(
    task_id='dummy_gold_copying_end',
    dag=db_dag
)


# dimension tables graph
for table in table_list:
    dummy_db_export_start >> table_export_group(table) >> dummy_db_export_end >>\
        copy_to_silver_group(table, dim_partitions_count) >> dummy_silver_copying_end >>\
        copy_to_gold_group(table) >> dummy_gold_copying_end


# order table
dummy_db_export_start >> facts_export >> dummy_db_export_end >>\
         copy_to_silver_group('orders', fact_partitions_count) >> dummy_silver_copying_end >>\
         orders_to_gold_operator >> dummy_gold_copying_end


# api
api_operator >> oos_to_silver_operator >> oos_to_gold_operator
