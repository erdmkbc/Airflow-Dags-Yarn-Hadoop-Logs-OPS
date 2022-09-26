from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from hadoop.yarn import Yarn
from airflow.operators.hive_operator import HiveOperator
import xml.etree.ElementTree as ET
import xml.dom.minidom
import json
import pandas as pd

from datetime import datetime

from airflow.operators.python_operator import PythonOperator

import sqlite3
import json
from yaml import serialize


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

# Variables extracts from airflow env
DB_NAME = Variable.get('database')
TB_NAME = Variable.get('tablename')
HDFS_FILE_PATH = Variable.get('hdfs_file_path')
LOCAL_FILE_PATH_YARN = Variable.get('logs_local_file_path_yarn')
LOCAL_FILE_PATH_DAGS = Variable.get('logs_local_file_path_dags')
BASH_FILE_PATH = Variable.get('bash_files_path')

# Local to hadoop -> yarn logs 
flying_file_manager_yarn = "hdfs dfs -put -f {local_file_path}/yarn_logs.csv {hdfs_file_path}".format(local_file_path = LOCAL_FILE_PATH_YARN,
                                                                                                      hdfs_file_path = HDFS_FILE_PATH)

# Local to hadoop -> dags logs 
flying_file_manager_dags = "hdfs dfs -put -f {local_file_path}/dags_running_time_logs.csv {hdfs_file_path}".format(local_file_path = LOCAL_FILE_PATH_DAGS,
                                                                                                                  hdfs_file_path = HDFS_FILE_PATH)

# local file paths for csv dags logs and yarn logs 
local_file_path_yarn = '{local_file_path}/yarn_logs.csv'.format(local_file_path = LOCAL_FILE_PATH_YARN)
local_file_path_dags_running = '{local_file_path}/dag_running_times.csv'.format(local_file_path = LOCAL_FILE_PATH_DAGS)
local_file_path_failure_tasks = '{local_file_path}/failure_tasks_last.csv'.format(local_file_path = LOCAL_FILE_PATH_DAGS)

dag = DAG(
    'resource_manager_logs',
    default_args=default_args,
    description='logs-management',
    schedule_interval='@once',
    #dagrun_timeout=timedelta(minutes=60),
)

# yarn resources ddl schema 
hql_create_table = """
CREATE TABLE IF NOT EXISTS logs_resources.yarn_logs_table(
                                                    nodes string, 
                                                    id string, 
                                                    `user` string, 
                                                    name string, 
                                                    queue string, 
                                                    state string, 
                                                    finalStatus string, 
                                                    progress float,
                                                    trackingUI string, 
                                                    trackingUrl string, 
                                                    diagnostics string, 
                                                    clusterId int, 
                                                    priority int,  
                                                    startedTime int,  
                                                    finishedTime int,  
                                                    elapsedTime int,  
                                                    amContainerLogs string, 
                                                    amHostHttpAddress string, 
                                                    masterNodeId string, 
                                                    allocatedMB int,  
                                                    allocatedVCores int,
                                                    runningContainers int
                                                    ) row format delimited fields terminated by ',' stored as textfile
"""

hql_insert_elements = "load data inpath '{hdfs_local_path}/yarn_logs.csv' into table logs_resources.yarn_logs_table;".format(hdfs_local_path = HDFS_FILE_PATH) 


def yarn_connection_rest_api(**kwargs):

    # connection and configration response type
    yarn = Yarn("http://localhost:8088", 'json')

    response_obj = yarn.cluster_applications({"limit":100})

    if yarn.response_type == 'json':
        print(json.dumps(response_obj, indent=4, sort_keys=True))
    elif yarn.response_type == 'xml':
        print(xml.dom.minidom.parseString(ET.tostring(response_obj)).toprettyxml())

    kwargs['ti'].xcom_push(key = 'formatted_processed_data', value = response_obj)

def yarn_json_response_parser(**kwargs):

    # pulling from xcoms json data logs type of tuple
    response_object_json = json.dumps(kwargs['ti'].xcom_pull(key = 'formatted_processed_data', task_ids = ['yarn_log_xcom_push']))

    # tuple to list
    response_object_json = json.loads(response_object_json)
    
    # list to dict
    response_object_json = response_object_json[0]

    # parse to dataframe 
       # pull the apps responses from json
    response_object_json = response_object_json['apps']

    # create to list fro explode the nested json when we are parseing to nested json from to dataframe
    response_obj_list = []
    
    for i in response_object_json.items():
        response_obj_list.append([i[0],i[1]])

    response_obj_df = pd.DataFrame(response_obj_list, columns = ['nodes', 'k'])

    # explode the nested columns from data frame
    response_obj_df = response_obj_df.explode('k')

    # normalize the data frame and last section fro parseing
    response_obj_df = pd.json_normalize(json.loads(response_obj_df.to_json(orient = 'records')))


    # filter yarn data columns
    df_filterred_columns = response_obj_df[['nodes', 
                                            'k.id', 
                                            'k.user', 
                                            'k.name', 
                                            'k.queue', 
                                            'k.state', 
                                            'k.finalStatus', 
                                            'k.progress',
                                            'k.trackingUI', 
                                            'k.trackingUrl', 
                                            'k.diagnostics', 
                                            'k.clusterId', 
                                            'k.priority',  
                                            'k.startedTime',  
                                            'k.finishedTime',  
                                            'k.elapsedTime',  
                                            'k.amContainerLogs', 
                                            'k.amHostHttpAddress', 
                                            'k.masterNodeId', 
                                            'k.allocatedMB',  
                                            'k.allocatedVCores', 
                                            'k.runningContainers' 
                                            ]] 
    # to csv and fly to local yarn_logs dir
    df_filterred_columns.to_csv(local_file_path_yarn)

    return print(response_obj_df.info())

    ############################# Airflow dag logs #############################  
def airflow_dags_logs(*args):
    
    # connection with airflow db via sqlite
    con = sqlite3.connect("/home/train/airflow/airflow.db")

    
    ######## en son zamanda ki fail tasklarının çıkarılımı
    failed_tasks_last_time =  pd.read_sql('''SELECT 
                                                    task_id,
                                                    dag_id,
                                                    execution_date
                                                        FROM task_instance
                                                    WHERE state = 'failed' AND 
                                                    execution_date BETWEEN datetime('now', 'start of day') AND datetime('now', 'localtime') ''', con = con)
    
        ######## en son zamanda ki fail tasklarının çıkarılımı
    running_times_tasks =  pd.read_sql('''  SELECT  
                                            execution_date,  
                                            MIN(start_date) AS start,  
                                            MAX(end_date) AS end,  
                                            MAX(end_date) - MIN(start_date) AS duration
                                                FROM  task_instance
                                                    WHERE  dag_id = 'resource_manager_logs' AND  state = 'success'
                                            GROUP BY  execution_date
                                            ORDER BY  execution_date DESC''', con = con)

    #### Flying to local landing file    
    running_times_tasks.to_csv(local_file_path_dags_running)
    failed_tasks_last_time.to_csv(local_file_path_failure_tasks)

    #############################     #############################     ############################# 


############################################### Task ordering ###############################################

    ################################## Yarn logs tasks ################################## 
yarn_log_push_xcom = PythonOperator(
                                    task_id = 'yarn_log_xcom_push',
                                    provide_context=True,
                                    python_callable= yarn_connection_rest_api,
                                    dag = dag
                                    )

yarn_logs_parser = PythonOperator(
                                task_id = 'yarn_logs_parser',
                                provide_context=True,
                                python_callable= yarn_json_response_parser,
                                dag = dag
                                )

logs_fly_to_hdfs_yarn = BashOperator(
                                task_id = 'logs_fly_to_hdfs',
                                provide_context = True,
                                bash_command = flying_file_manager_yarn,
                                dag = dag
                                )
create_table_schema = HiveOperator(
                                task_id = 'create_table_schema',
                                provide_context = True,
                                hql = hql_create_table,
                                dag = dag
                                )
insert_table_schema = HiveOperator(
                                task_id = 'insert_table',
                                provide_context= True,
                                hql = hql_insert_elements, 
                                dag = dag
                                )
   #############################################################################################
logs_fly_to_hdfs_dags = BashOperator(
                                task_id = 'logs_fly_to_hdfs_dags',
                                provide_context = True,
                                bash_command = flying_file_manager_dags,
                                dag = dag
                                )
dag_tasks_runing_time = PythonOperator(
    
    task_id = 'running_times_dags',
    depends_on_past=False,
    python_callable=airflow_dags_logs,
    dag = dag
)

# tasks ordering -> yarn logs 

yarn_log_push_xcom >> yarn_logs_parser >> logs_fly_to_hdfs_yarn >> create_table_schema >> insert_table_schema

# tasks ordering -> airflow dags logs

logs_fly_to_hdfs_dags >> dag_tasks_runing_time
#####################################################################################################

# tasks - file lifcycle data to hive tables

file_life_cycle = BashOperator(
    task_id='hadoop_file_life_cycle',
    bash_command = 'bash /home/train/airflow/bash_commands/hdfs_remove_file_by_day.sh ',
    #bash_command="{bash_files_path}/hdfs_remove_file_by_day.sh".format(bash_files_path = BASH_FILE_PATH),
    dag=dag)

file_life_cycle







