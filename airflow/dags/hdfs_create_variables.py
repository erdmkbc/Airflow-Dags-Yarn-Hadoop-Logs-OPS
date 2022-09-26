#from airflow.models import Variable
#from airflow import DAG
#from airflow.utils.dates import days_ago
#from datetime import timedelta
#
#from airflow.operators.python_operator import PythonOperator
#
#import json
#
#
#default_args = {
#    'owner': 'airflow',
#    'depends_on_past': False,
#    'start_date': days_ago(1),
#    'email': ['airflow@example.com'],
#    'email_on_failure': False,
#    'email_on_retry': False,
#    'retry_delay': timedelta(minutes=5)
#}
#
############################### Airflow Load Json Variable To Airflow Env ##############################
#
#def variable_set(**kwargs):
#    
#    # open and read json file
#    f = open('/home/train/airflow/json_keys/directories.json')
#    value = json.load(f)
#
#    # add variables using Airflow API
#    Variable.set(key="direct_keys", value=value, serialize_json=True)
#
#########################################################################################################
#
#
#dag = DAG(
#    'hdfs_create_variables',
#    default_args=default_args,
#    description='001-task',
#    schedule_interval='@once',
#    #dagrun_timeout=timedelta(minutes=60),
#)
#
#
######################### Create table partitions directory in hdfs ###################################

#set_variable = PythonOperator( task_id='set_variable',
#                                depends_on_past=False,
#                                retries=1,
#                                python_callable=variable_set,
#                                provide_context=True,
#                                dag=dag)
#
#set_variable
