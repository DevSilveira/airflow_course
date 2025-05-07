from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

with DAG('producer', description='producer',
         schedule_interval=None, start_date=datetime(2025,5,6),
         catchup=False) as dag:
    
    mydataset = Dataset('/opt/airflow/data/Churn_new.csv')
    def my_file():
        dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
        dataset.to_csv('/opt/airflow/data/Churn_new.csv', sep=';', index=False)

    t1 = PythonOperator(task_id='t1', python_callable=my_file, outlets=[mydataset])

    t1
