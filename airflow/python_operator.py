from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import zipfile

def unzip():
    with zipfile.ZipFile('data/zip', 'r') as zip_ref:
        zip_ref.extractall('data/zip')

default_args = { 
    'owner': 'Josh',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1, 
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('my_dag', default_args=default_args) 

t1 = BashOperator(
    task_id='spider', 
    bash_command='python spider.py', 
    dag=dag)

t2 = BashOperator(
    task_id='unzip_file', 
    bash_command=unzip, 
    dag=dag)

t3 = BashOperator(
    task_id='spark_etl', 
    bash_command='python spark_etl.py', 
    dag=dag)
    
t1 >> t2 >> t3