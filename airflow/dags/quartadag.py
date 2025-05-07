from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG('quarta_dag', description="Quarta DAG",
          schedule_interval=None, start_date=datetime(2025, 4, 28),
          catchup=False) as dag:

    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5")
    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5")
    task3 = BashOperator(task_id="tsk3", bash_command="sleep 5")

    # Outra maneira de definir questão de precedência
    task1.set_upstream(task2)
    task2.set_upstream(task3)
