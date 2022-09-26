############################## İmport işlemleri ##############################
 
# DAG komponentinin import edilmesi ve scheduler time kontrolü için time paketlerinin import edilmesi
from datetime import timedelta
from airflow import DAG
# airflow metadb'ye bağlanılması 
import sqlite3
# yapılacak dosya işlemleri için bash komutları, airflow komutları için taskların bash komutları
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# python taskları dönüşü için 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# metadb verilerinin okunması için pandasın import edilmesi 
import pandas as pd 

###############################################################################
from airflow.sensors.hdfs_sensor import HdfsSensor
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    'HdfsSensor_test', default_args=default_args, schedule_interval='@once')


source_data_sensor = HdfsSensor(
    task_id='hdfs_source_data_sensor',
    filepath='/user/train/expectancy_main_2022-07-24',
    hdfs_conn_id='webhdfs_default',
    poke_interval=3,
    trigger_rule='one_success',
    timeout=300,
    dag=dag
)

source_data_sensor



