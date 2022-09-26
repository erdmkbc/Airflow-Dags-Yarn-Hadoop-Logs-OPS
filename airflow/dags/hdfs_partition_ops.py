#from tkinter.tix import COLUMN
#from airflow.models import Variable
#from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
#from airflow.utils.dates import days_ago
#from datetime import timedelta
#
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.sensors import ExternalTaskSensor
#from airflow.operators.hive_operator import HiveOperator
#
#from airflow.sensors.hdfs_sensor import HdfsSensor
#
#import json
#from yaml import serialize
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
############################### Define Variables From Airflow ENV #######################################
## get variables using Airflow API
#    
#VARIABLES = Variable.get('direct_keys', deserialize_json=True)
#
#HDFS_DATA_DIR = VARIABLES["hdfs_directories_main"]
#LOCAL_DATA_DIR = VARIABLES["local_directories_main"]
#STATIC_PARTITON_COLUMN = VARIABLES['partition_column']
#STATIC_PARTITON_ELEMENT_1 = VARIABLES['static_partition_element']
#STATIC_PARTITON_ELEMENT_2 = VARIABLES['static_partition_element_2']
#TABLE_NAME = VARIABLES["table_name_main"]
#
#########################################################################################################
#
#dag_target = DAG(
#    'hdfs_partition_ops',
#    default_args=default_args,
#    description='001-task',
#    schedule_interval='@once',
#    #dagrun_timeout=timedelta(minutes=60),
#)
#
################################# External task sensor ################################################## -SUCCES  
#triggered_sensor_var = ExternalTaskSensor(
#                    dag = dag_target,
#                    task_id = 'Trigger_waiting',
#                    #retries = 2,
#                    #retry_delay = timedelta(seconds = 30),
#                    #mode = 'reschedule',
#                    external_dag_id= 'hdfs_create_variables',
#                    external_task_id='set_variable'
#)
########################## Create table's partitions directory in hdfs ################################### -DEPCREATED   
#                                                                                                                                                                                                                                                                                                                
#create_main_files_directories = "hdfs dfs -mkdir {hdfs_data_dir}/{table_name}_".format(hdfs_data_dir = HDFS_DATA_DIR, 
#                                                                                       table_name = TABLE_NAME) + "{{ ds }}" + "&&" + "hdfs dfs -mkdir {hdfs_data_dir}/{table_name}_".format(hdfs_data_dir = HDFS_DATA_DIR, table_name = TABLE_NAME) + "{{ ds }}" + "/developing" + "&&" + "hdfs dfs -mkdir {hdfs_data_dir}/{table_name}_".format(hdfs_data_dir = HDFS_DATA_DIR, table_name = TABLE_NAME) + "{{ ds }}" + "/developed"   
#create_main_data_dir = BashOperator(
#                                task_id = 'create_main_daily_dir',
#                                retries=1,
#                                bash_command= create_main_files_directories,
#                                provide_context=True,
#                                dag=dag_target)
#
###################### Local main data to hdfs  ############################################################ -SUCCES 
#
#put_main_file_from_local = "hdfs dfs -put {local_data_dir}/{table_name}.csv".format(local_data_dir = LOCAL_DATA_DIR, 
#                                                                                    table_name = TABLE_NAME) + " " + "{hdfs_data_dir}/{table_name}_".format(hdfs_data_dir = HDFS_DATA_DIR, 
#                                                                                                                                                            table_name = TABLE_NAME) + "{{ ds }}"
#
#put_file_in_hdfs = BashOperator(
#                                task_id = 'local_to_hdfs',
#                                depends_on_past=False,
#                                bash_command=put_main_file_from_local,
#                                retries=1,
#                                provide_context = True,
#                                dag = dag_target
#)
#########################################################################################################
################################################# Hive tables creations ################################ -SUCCES
#create_main_table = HiveOperator(
#                    task_id='create_main_table',
#                    hql='''
#                        CREATE EXTERNAL TABLE IF NOT EXISTS expectancy_main(
#                            Country string,
#                            year int,
#                            Status string,
#                            Life_expectancy float,
#                            Adult_Mortality int,
#                            infant_deaths int,
#                            Alcohol float,
#                            percentage_expenditure float,
#                            Hepatitis_B int,
#                            Measles int,
#                            BMI float,
#                            under_five_deaths int,
#                            Polio int,
#                            Total_expenditure float,
#                            Dipthheria int,
#                            HIV_AIDS float,
#                            GDP string,
#                            Population string,
#                            thinness_1_19_years float,
#                            thinness_5_9_years float,
#                            Income_composition_of_resources float,
#                            Schooling float
#                        )   row format delimited fields terminated by ',' stored as textfile location '{hdfs_path}/{table_name}_'''.format(hdfs_path = HDFS_DATA_DIR, 
#                                                                                                                                          table_name = TABLE_NAME) + "{{ ds }}'", 
#                    dag=dag_target
#)
#
#hql = "load data inpath '{hdfs_data_dir}/{table_name}_".format(hdfs_data_dir = HDFS_DATA_DIR,table_name = TABLE_NAME) + "{{ ds }}/" + "{table_name}.csv'".format(table_name = TABLE_NAME) +  " " + "into table {table_name}".format(table_name = TABLE_NAME)
#
#load_data_to_hive_table = HiveOperator(
#                        task_id = 'load_data_to_main_table',
#                        hql = hql,
#                        provide_context = True,
#                        dag = dag_target
#)
#
#create_table_parquet_format = HiveOperator(
#                    task_id='create_parquet_table',
#                    hql='''
#                        CREATE EXTERNAL TABLE IF NOT EXISTS expectancy_main_parquet(
#                            Country string,
#                            year int,
#                            Status string,
#                            Life_expectancy float,
#                            Adult_Mortality int,
#                            infant_deaths int,
#                            Alcohol float,
#                            percentage_expenditure float,
#                            Hepatitis_B int,
#                            Measles int,
#                            BMI float,
#                            under_five_deaths int,
#                            Polio int,
#                            Total_expenditure float,
#                            Dipthheria int,
#                            HIV_AIDS float,
#                            GDP string,
#                            Population string,
#                            thinness_1_19_years float,
#                            thinness_5_9_years float,
#                            Income_composition_of_resources float,
#                            Schooling float
#                        )   stored as parquet''',
#                    dag=dag_target
#)
#
#hql = 'insert into table {}_parquet select * from {}'.format(TABLE_NAME, TABLE_NAME)
#
#insert_parquet_file = HiveOperator(
#                    task_id = 'insert_data_parquet_file',
#                    hql = hql,
#                    dag = dag_target         
#)
#create_partition_table_static = HiveOperator(
#                    task_id='create_partition_tables_static',
#                    hql='''
#                        CREATE TABLE IF NOT EXISTS expectancy_main_partition(
#                            Country string,
#                            year int,
#                            Life_expectancy float,
#                            Adult_Mortality int,
#                            infant_deaths int,
#                            Alcohol float,
#                            percentage_expenditure float,
#                            Hepatitis_B int,
#                            Measles int,
#                            BMI float,
#                            under_five_deaths int,
#                            Polio int,
#                            Total_expenditure float,
#                            Dipthheria int,
#                            HIV_AIDS float,
#                            GDP string,
#                            Population string,
#                            thinness_1_19_years float,
#                            thinness_5_9_years float,
#                            Income_composition_of_resources float,
#                            Schooling float
#                        )  partitioned by (Status string) stored as parquet''',
#                    dag=dag_target
#)
#
#hql = """insert into table {inserting_table}_partition partition({partition_column} = '{static_partition_element}') 
#select  
#                            Country, 
#                            year,
#                            Life_expectancy,
#                            Adult_Mortality,
#                            infant_deaths,
#                            Alcohol,
#                            percentage_expenditure,
#                            Hepatitis_B,
#                            Measles,
#                            BMI,
#                            under_five_deaths,
#                            Polio,
#                            Total_expenditure,
#                            Dipthheria,
#                            HIV_AIDS,
#                            GDP,
#                            Population,
#                            thinness_1_19_years,
#                            thinness_5_9_years,
#                            Income_composition_of_resources,
#                            Schooling
#from {inserted_table} where {partition_column} = '{static_partition_element}';
#
#insert into table {inserting_table}_partition partition({partition_column} = '{static_partition_element_2}') 
#select  
#                            Country, 
#                            year,
#                            Life_expectancy,
#                            Adult_Mortality,
#                            infant_deaths,
#                            Alcohol,
#                            percentage_expenditure,
#                            Hepatitis_B,
#                            Measles,
#                            BMI,
#                            under_five_deaths,
#                            Polio,
#                            Total_expenditure,
#                            Dipthheria,
#                            HIV_AIDS,
#                            GDP,
#                            Population,
#                            thinness_1_19_years,
#                            thinness_5_9_years,
#                            Income_composition_of_resources,
#                            Schooling
#from {inserted_table} where {partition_column} = '{static_partition_element_2}'""".format(inserting_table = TABLE_NAME,
#                                                                                          inserted_table = TABLE_NAME,
#                                                                                          partition_column = STATIC_PARTITON_COLUMN,
#                                                                                          static_partition_element = STATIC_PARTITON_ELEMENT_1,
#                                                                                          static_partition_element_2 = STATIC_PARTITON_ELEMENT_2
#                                                                                        )
#
#insert_partition_tables_static = HiveOperator(
#
#                    task_id = 'insert_status_partition_tables_statical',
#                    hql = hql,
#                    dag = dag_target
#)
#
#
#create_partition_table_dynamic = HiveOperator(
#                    task_id='create_partition_tables_dynamic',
#                    hql='''
#                        CREATE TABLE IF NOT EXISTS expectancy_main_partition_dynamic(
#                            Country string,
#                            year int,
#                            Life_expectancy float,
#                            Adult_Mortality int,
#                            infant_deaths int,
#                            Alcohol float,
#                            percentage_expenditure float,
#                            Hepatitis_B int,
#                            Measles int,
#                            BMI float,
#                            under_five_deaths int,
#                            Polio int,
#                            Total_expenditure float,
#                            Dipthheria int,
#                            HIV_AIDS float,
#                            GDP string,
#                            Population string,
#                            thinness_1_19_years float,
#                            thinness_5_9_years float,
#                            Income_composition_of_resources float,
#                            Schooling float
#                        )  partitioned by (Status string) stored as parquet''',
#                    dag=dag_target
#)
#
#hql = """insert into table {main_table}_partition_dynamic partition(Status) 
#        select             
#        Country, 
#        year,
#        Life_expectancy,
#        Adult_Mortality,
#        infant_deaths,
#        Alcohol,
#        percentage_expenditure,
#        Hepatitis_B,
#        Measles,
#        BMI,
#        under_five_deaths,
#        Polio,
#        Total_expenditure,
#        Dipthheria,
#        HIV_AIDS,
#        GDP,
#        Population,
#        thinness_1_19_years,
#        thinness_5_9_years,
#        Income_composition_of_resources,
#        Schooling,
#        status
#        
#        from {main_table}_parquet""".format(main_table = TABLE_NAME)
#
#insert_partition_tables_dynamic = HiveOperator(
#                        task_id = 'insert_partition_tables_dynamic',
#                        hql = hql,
#                        dag = dag_target
#                        )
###################################### Bucketting partition table ######################################
#create_table_partition_buck = HiveOperator(
#            task_id = 'create_table_partition_buck',
#            hql = """CREATE TABLE IF NOT EXISTS expectancy_main_partition_buck(
#                            Country string,
#                            year int,
#                            Life_expectancy float,
#                            Adult_Mortality int,
#                            infant_deaths int,
#                            Alcohol float,
#                            percentage_expenditure float,
#                            Hepatitis_B int,
#                            Measles int,
#                            BMI float,
#                            under_five_deaths int,
#                            Polio int,
#                            Total_expenditure float,
#                            Dipthheria int,
#                            HIV_AIDS float,
#                            GDP string,
#                            Population string,
#                            thinness_1_19_years float,
#                            thinness_5_9_years float,
#                            Income_composition_of_resources float,
#                            Schooling float
#                        )  partitioned by (Status string) clustered by (year) into 4 buckets stored as parquet """,
#           dag = dag_target
#)
#
#hql = '''insert into table {main_table}_partition_buck partition(Status) 
#        select             
#            Country, 
#            year,
#            Life_expectancy,
#            Adult_Mortality,
#            infant_deaths,
#            Alcohol,
#            percentage_expenditure,
#            Hepatitis_B,
#            Measles,
#            BMI,
#            under_five_deaths,
#            Polio,
#            Total_expenditure,
#            Dipthheria,
#            HIV_AIDS,
#            GDP,
#            Population,
#            thinness_1_19_years,
#            thinness_5_9_years,
#            Income_composition_of_resources,
#            Schooling,
#            status
#        
#        from {main_table}_parquet'''.format(main_table = TABLE_NAME)
#
#
#insert_bucketting_tables = HiveOperator(
#            task_id = 'insert_bucketing_tables',
#            hql = hql,
#            dag = dag_target  
#)
############ depcretaded for hive 3.0 version ##############
##hql = "create index i1 on table {table_name}_parquet({column_name}) as '{index_type}' with deferred rebuild".format(
##                                                                                                            table_name = TABLE_NAME,
##                                                                                                            column_name = INDEXING_COLUMN,
##                                                                                                            index_type = INDEXING_TYPE
##                                                                                                            )
##
##create_index_table = HiveOperator(
##            task_id = 'create_index_table',
##            hql = hql,
##            dag = dag_target
##)
##
##hql = "alter index {index_name} on {table_name} rebuild".format(
##                                                                index_name = INDEX_NAME,
##                                                                table_name = TABLE_NAME
##                                                                )
##
##alter_index_table = HiveOperator(
##            task_id = 'alter_index_table',
##            hql = hql,
##            dag = dag_target
##)
#
#
#
## load data from csv file                                                                   ## stored as parquet file
#triggered_sensor_var >> create_main_data_dir >> put_file_in_hdfs >> create_main_table >> load_data_to_hive_table >> [create_table_parquet_format >> insert_parquet_file,
#
### create partition static
#create_partition_table_static >> insert_partition_tables_static, 
##
### create partition dynamic
#create_partition_table_dynamic >> insert_partition_tables_dynamic, 
##
### create partition and bucket
#create_table_partition_buck >> insert_bucketting_tables]

# create index in table
# create_index_table >> alter_index_table Depcrated for hive 3.0 




################################################### Deprcreated for a now ##################################################
##filepath = "/user/train/hadoop_daily_part_files"
##
##check_succes_marking = HdfsSensor(
##                                task_id='check_succes_marking',
##                                filepath=filepath,
##                                hdfs_conn_id='webhdfs_default',
##                                dag=dag
##)
#
##set_variable >> create_partition_dir 
##############################################################################################################################
#

