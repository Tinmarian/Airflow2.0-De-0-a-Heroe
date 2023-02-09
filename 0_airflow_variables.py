


# DEL DAG 0_airflow_macro...


from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.models import Variable

from datetime import date
from random import random


# DAG 
# --------------------------------------------------------------->

# TASKS pertenecen a un DAG:
#    TASKS_1(., ., ., dag=mi_dag) --> TASKS_2(., ., ., dag=mi_dag)


# VARIABLES
var_name = 'example_var'
var = Variable.get(f'{var_name}',deserialize_json=True)

OWNER = var['owner']
MENSAJE = var['mensaje']
INTERVAL = var['intervalo']

"""
{
    "owner":"Tinmar",
    "mensaje":"Mi primer DAG utilizando variables",
    "intervalo":"00 12 * * *"
}
"""

default_args = {
    'owner': OWNER,
    'start_date': days_ago(1)
}
 
dag_args = {
    'dag_id': '0_airflow_variables',
    'schedule_interval': INTERVAL,
    'catchup': False,
    'default_args': default_args,
    'user_defined_macros': {
        'mensaje': MENSAJE,
        'fecha': str(date.today())
    }
}


with DAG(**dag_args,tags=['Curso_1']) as dag:

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "---> {{ mensaje }}, Fecha: $TODAY y TIMESTAMP ES: {{ ts_nodash_with_tz }}"',
        env={ 'TODAY': '{{ fecha }}' }
    )

    def print_random_number(number=None, otro=None):
        for i in range(number):
            print(f'ESTE ES EL RANDOM NUMBER {i+1}', random())
    
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_random_number,
        op_kwargs={ 'number': 10 }
    )


# DEPENDENCIAS
bash_task >> python_task