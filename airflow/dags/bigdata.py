from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator

with DAG('bigdata', description='bigdata',
         schedule_interval=None, start_date=datetime(2025,5,6),
         catchup=False) as dag:
    
    big_data = BigDataOperator(task_id='big_data',
                               path_to_csv_file='/opt/airflow/data/Churn.csv',
                               path_to_save_file='/opt/airflow/data/Churn.json',
                               file_type='json')
    
    big_data