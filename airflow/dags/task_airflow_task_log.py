############################## İmport işlemleri ############################## 
from datetime import timedelta
from airflow import DAG
 
import sqlite3

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
 
import pandas as pd 

 

############################## Code Section ############################## 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

################### task loglarının tablolarının oluşturulması ################### 
def fail_tasks_table_prepration(*args):
    
    # metadb ile bağlanıtının kurulması 
    con = sqlite3.connect("/home/train/airflow/airflow.db")

    ######### fail taskların tespit edilmesi - genel
    failed_tasks = pd.read_sql('''SELECT 
                                    task_id,
                                    dag_id,
                                    execution_date,  
                                    FROM task_instance
                                    WHERE state = 'failed' ''', con = con)
    
    # email eki oluşturularak txt'ye yazılması
    with open('fail_logs', 'a') as f:
        
        failed_tasks_string = failed_tasks.to_string(header=False, index=False)
        f.write(failed_tasks_string)
    
    ######## en son zamanda ki fail tasklarının çıkarılımı
        failed_tasks_last_time =  pd.read_sql('''SELECT 
                                                    task_id,
                                                    dag_id,
                                                    execution_date
                                                        FROM task_instance
                                                    WHERE state = 'failed' AND 
                                                    execution_date BETWEEN datetime('now', 'start of day') AND datetime('now', 'localtime') ''', con = con)
    
    # email eki oluşturularak txt'ye yazılması
    with open('fail_logs_last_time', 'w') as f:
        
        failed_tasks_string = failed_tasks.to_string(header=False, index=False)
        f.write(failed_tasks_last_time)

    ######## succes ve fail de olan taskların sayıları - genel ve son zaman
    failed_tasks_last_time =  pd.read_sql('''SELECT 
                                                    COUNT(state) as task_state_cnt,
                                                    task_id,
                                                    dag_id,
                                                    execution_date,
                                                    state,
                                                    'general' as analyz_type
                                                        FROM task_instance 
                                                    GROUP BY 2,3,4,5,6
                                                    UNION ALL 
                                            SELECT 
                                                    COUNT(state) as task_state_cnt,
                                                    task_id,
                                                    dag_id,
                                                    execution_date,
                                                    state,
                                                    'last_time' as analyz_type
                                                        FROM task_instance 
                                                    GROUP BY 2,3,4,5,6
                                                    WHERE execution_date BETWEEN datetime('now', 'start of day') AND datetime('now', 'localtime')   
                                                    GROUP BY 2,3,4''', con = con)
    
    # email eki oluşturularak txt'ye yazılması
    with open('fail_logs_aggregation', 'a') as f:
        
        failed_tasks_string = failed_tasks.to_string(header=False, index=False)
        f.write(failed_tasks_last_time)


######################## Log analizi fonksiyonun dage takılması ############################## 
dag = DAG(
    'task_logs_DAG',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)


task_logs_attachs_analyz = PythonOperator(
    task_id = 'analyz_logs',
    depends_on_past=False,
    python_callable=fail_tasks_table_prepration,
    dag = dag
)


task_logs_attachs_analyz




















