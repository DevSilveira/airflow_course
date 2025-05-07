from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG('bancodedados', description='bancodedados',
         schedule_interval=None, start_date=datetime(2025,5,6),
         catchup=False) as dag:
    
    create_table = PostgresOperator(task_id='create_table',
                                    postgres_conn_id='postgres',
                                    sql='create table if not exists teste(id int);')
    
    insert_data = PostgresOperator(task_id='insert_data',
                                    postgres_conn_id='postgres',
                                    sql='insert into teste values(1);')
    
    query_data = PostgresOperator(task_id='query_data',
                                    postgres_conn_id='postgres',
                                    sql='select * from teste')
    
    def print_result(ti):
        task_instance = ti.xcom_pull(task_ids='query_data')
        print(f'Resultado da consulta:')
        for row in task_instance:
            print(row)
    print_result_task = PythonOperator(task_id='print_result_task',
                                       python_callable=print_result, provide_context=True)
    
    create_table >> insert_data >> query_data >> print_result_task
