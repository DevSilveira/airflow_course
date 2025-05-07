from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime

with DAG('httpsensor', description='httpsensor',
         schedule_interval=None, start_date=datetime(2025,5,6),
         catchup=False) as dag:
    
    def query_api():
        import requests
        response = requests.get('https://api.adviceslip.com/advice')
        print(response.text)
    
    check_api = HttpSensor(task_id='check_api', http_conn_id='connection', endpoint='advice',
                           poke_interval=5, timeout=20)
    
    process_data = PythonOperator(task_id='process_data', python_callable=query_api)

    check_api >> process_data