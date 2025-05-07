from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG('dagrundag2', description="Dag run dag",
          schedule_interval=None, start_date=datetime(2025,5,5),
          catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', dag=dag)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 5', dag=dag)

task1 >> task2
