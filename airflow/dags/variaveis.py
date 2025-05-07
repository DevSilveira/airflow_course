from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

dag = DAG('variaveis', description="variaveis",
          schedule_interval=None, start_date=datetime(2025,5,5),
          catchup=False)

def print_variable(**context):
    minha_var = Variable.get('minhavar')
    print(f'O valor da variável é: {minha_var}')

task1 = PythonOperator(task_id='tsk1', python_callable=print_variable, dag=dag)

task1
