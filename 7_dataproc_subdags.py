

from airflow import DAG

from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from airflow.operators.python import BranchPythonOperator

from airflow.operators.subdag import SubDagOperator
from pyspark_subdag import mySubDag

from random import uniform


default_args = {
    'owner': 'David Sanchez',
    'start_date': days_ago(1)
}

dag_args = {
    'dag_id': '7_dataproc_subdags',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}

# Python Operator
def number_task(min_num=None, max_num=None):
    #return 'par_task' if round(uniform(min_num, max_num)) % 2 == 0 else 'impar_task'
    return 'par_task'


with DAG(**dag_args,tags=['Curso_1']) as dag:

# TASK 1: CREATE CLUSTER --> OPERATOR
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id='regal-oasis-291423',
        cluster_name='spark-cluster-123',
        num_workers=2,
        storage_bucket='spark-bucket-987',
        region='us-east1',
    )


# TASK 2: IDENTIFICAR NÚMERO --> OPERATOR (PYTHON)
    iden_number = BranchPythonOperator(
        task_id='iden_number',
        python_callable=number_task,
        op_kwargs={
            'min_num': 1, 
            'max_num': 100
        }
    )


# TASK 3: PYSPARK JOBS

    ## TASK 3.1: EJECUTAR PYSPARK (IMPAR) --> OPERATOR
    pyspark_job = {
        'reference': {
            'project_id': 'regal-oasis-291423',
            'job_id': '10ad560c_mainjob_std'
        },
        'placement': {
            'cluster_name': 'spark-cluster-123'
        },
        'labels': {
            'airflow-version': 'v2-1-0'
        },
        'pyspark_job': {
            'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
            'main_python_file_uri': 'gs://spark-bucket-987/pyspark/impar_task/vars_stdp.py'
        }
    }

    impar_task = DataprocSubmitJobOperator(
        task_id='impar_task',
        project_id='regal-oasis-291423',
        location='us-east1',
        job=pyspark_job,
        gcp_conn_id='google_cloud_default'
    )

    ## TASK 3.2: EJECUTAR PYSPARK (PAR) --> OPERATOR
    subdag_args = {
        'parent_dag': '7_dataproc_subdags', 
        'task_id': 'par_task', 
        'schedule_interval': '@daily', 
        'default_args': default_args
    }

    par_task = SubDagOperator(
        task_id='par_task',
        subdag=mySubDag(**subdag_args)
    )


# TASK 4: DELETE CLUSTER --> OPERATOR
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id='regal-oasis-291423',
        cluster_name='spark-cluster-123',
        trigger_rule='all_done',
        region='us-east1'
    )


# Dependencies
(
    create_cluster 
    >> iden_number 
    >> [ impar_task , par_task ] 
    >> delete_cluster
)


