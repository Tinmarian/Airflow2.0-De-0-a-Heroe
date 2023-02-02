

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.operators.python import BranchPythonOperator
from random import uniform

# from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.dummy import DummyOperator

########################
# DataprocSubmitPySparkJobOperator (IMPAR TASK)

# pyspark_job = DataprocSubmitPySparkJobOperator(
#     task_id='pyspark_job',
#     project_id='regal-oasis-291423',
#     main='gs://airflow_spark_bucket/pyspark/impar_task/vars_stdp.py',
#     cluster_name='sparkcluster-987',
#     dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
#     region='us-east1'
# ).generate_job()

# pyspark_job

########################


default_args = {
    'owner': 'David S',
    'start_date': days_ago(1)
}

dag_args = {
    'dag_id': '6_dataproc_airflow',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}


# Branch Python Function
pyspark_files = ('avg_quant', 'avg_tincome', 'avg_uprice')

def number_task(min_number=None, max_number=None):
    #return 'par_task' if round(uniform(min_number, max_number)) % 2 == 0 else 'impar_task'

    #if round(uniform(min_number, max_number)) % 2 == 0:
    if False:
        return 'impar_task'
    else:
        return [f'par_task.{x}' for x in pyspark_files]


with DAG(**dag_args,tags=['Curso_1']) as dag:

    start_task = DummyOperator(task_id='start_task')
    
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id='serene-gradient-371719',
        cluster_name='airflow-spark-cluster',
        num_workers=2,
        storage_bucket='airflow_spark_bucket',
        region='us-central1'
    )

# BRANCHING: IDENTIFICAR NUMERO PAR O IMPAR
    iden_number = BranchPythonOperator(
        task_id='iden_number',
        python_callable=number_task,
        op_args=[1, 100]
    )


# EJECUTAR PYSPARK JOB SI (CONDICION BRANCH IMPAR)
    pyspark_job = {
        'reference': {
            'project_id': 'serene-gradient-371719',
            'job_id': 'IMPARTASK_dfa23fbf'
        },
        'placement': {
            'cluster_name': 'airflow-spark-cluster'
        },
        'labels': {
            'airflow-version': 'v2-3-0'
        },
        'pyspark_job': {
            'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
            'main_python_file_uri': 'gs://airflow_spark_bucket/impar_task/vars_stdp.py'
        }
    }

    impar_task = DataprocSubmitJobOperator(
        task_id='impar_task',
        project_id='serene-gradient-371719',
        region='us-central1',
        job=pyspark_job
    )


# EJECUTAR PYSPARK JOB SI (OTRA CONDICION BRANCH PAR)
    with TaskGroup(group_id='par_task') as par_task:

        pyspark_files = ('avg_quant', 'avg_tincome', 'avg_uprice')

        for subtask in pyspark_files:

            pyspark_subjob = {
                'reference': {
                    'project_id': 'serene-gradient-371719',
                    'job_id': 'PARTASK_dfa23fbf_{}'.format(subtask)
                },
                'placement': {
                    'cluster_name': 'airflow-spark-cluster'
                },
                'labels': {
                    'airflow-version': 'v2-3-0'
                },
                'pyspark_job': {
                    'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
                    'main_python_file_uri': f'gs://airflow_spark_bucket/par_task/{subtask}.py'
                }
            }

            DataprocSubmitJobOperator(
                task_id=subtask,
                project_id='serene-gradient-371719',
                region='us-central1',
                job=pyspark_subjob
            )


# DELETE CLUSTER TASK (TRIGGERS)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id='serene-gradient-371719',
        cluster_name='airflow-spark-cluster',
        region='us-central1',
        trigger_rule='all_done'
    )

    end_task = DummyOperator(task_id='end_task')

# DEPENDENCIAS
(
    start_task >> create_cluster
    >> iden_number
    >> [ impar_task, par_task ]
    >> delete_cluster >> end_task
)