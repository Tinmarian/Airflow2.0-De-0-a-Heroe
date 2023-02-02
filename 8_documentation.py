
'''
# Ejemplo de documentación en DAG de Airflow
Aquí mostramos cómo poner documentación en un DAG de Airflow. 
Aparecerá en formato Markdown.

* objeto 1
* objeto 2
* objeto 3
'''

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import date
from random import random

# DAG 
# --------------------------------------------------------------->

# TASKS pertenecen a un DAG:
#    TASKS_1(., ., ., dag=mi_dag) --> TASKS_2(., ., ., dag=mi_dag)

default_args = {
    'owner': 'David S',
    'start_date': days_ago(1)
}

dag_args = {
    'dag_id': '8_documentation',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}


with DAG(**dag_args,tags=['Curso_1']) as dag:

    dag.doc_md = __doc__

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "---> Mi primer DAG en Apache Airflow!, Fecha: $TODAY"',
        env={ 'TODAY': str(date.today()) }
    )

    bash_task.doc_md = (
        '''
        ### Bash operator task documentation
        Este es un bash operator con su documentación
        '''
    )

    def print_random_number(number=None, otro=None):
        for i in range(number):
            print(f'ESTE ES EL RANDOM NUMBER {i+1}', random())
    
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_random_number,
        op_kwargs={ 'number': 10 }
    )

    python_task.doc_md = (
        '''
        ### Python operator task documentation
        Este es un Python operator con su documentación
        '''
    )    

# DEPENDENCIAS
bash_task >> python_task

