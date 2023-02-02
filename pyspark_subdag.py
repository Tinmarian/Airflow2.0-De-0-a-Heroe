
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


def mySubDag(parent_dag=None, task_id=None, schedule_interval=None, default_args=None):

    subdag = DAG(
        dag_id='{}.{}'.format(parent_dag, task_id),
        schedule_interval=schedule_interval,
        default_args=default_args
    )

    pyspark_files = ('avg_quant', 'avg_tincome', 'avg_uprice')
    
    for subtask in pyspark_files:

        pyspark_subjob = {
            'reference': {
                'project_id': 'serene-gradient-371719',
                'job_id': f'b1b63d0a_subjob_{subtask}'
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
            job=pyspark_subjob,
            dag=subdag
        )

    return subdag