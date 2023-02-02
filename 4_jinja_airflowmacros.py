

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


# ARGUMENTS
default_args = {
    'owner': 'David Sanchez',
    'start_date': days_ago(1)
}

dag_args = {
    'dag_id': '4_jinja_airflowmacros',
    'schedule_interval': '@daily', 
    'catchup': False, 
    'max_active_runs': 1,
    'user_defined_macros': {
        'project': 'serene-gradient-371719',
        'dataset': 'airflow_trabajo',
        'orig_table': 'retail_years_jinja_macros',
        'dest_table': 'retail_years_resume_jinja_macros',
        'bucket': 'airflow23_bucket',
        'backup_bucket': 'airflow23_final_bucket'
    },
    'default_args': default_args
}


# HOOKS
def list_objects(bucket=None):
    return GCSHook().list(bucket)

def move_objects(source_bucket=None, destination_bucket=None, prefix=None, **kwargs):

    storage_objects = kwargs['ti'].xcom_pull(task_ids='list_files')

    for ob in storage_objects:
        dest_ob = ob

        if prefix:
            dest_ob = f'{prefix}/{ob}'

        GCSHook().copy(source_bucket, ob, destination_bucket, dest_ob)
        GCSHook().delete(source_bucket, ob)


# DAG
with DAG(**dag_args,tags=['Curso_1']) as dag:

    start_task = DummyOperator(task_id='Start')

    # TASK
    list_files = PythonOperator(
        task_id='list_files',
        python_callable=list_objects,
        op_kwargs={'bucket': '{{ bucket }}'}
    )

    # TASK
    cargar_datos = GCSToBigQueryOperator(
        task_id='cargar_datos',
        bucket='{{ bucket }}',
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=';',
        destination_project_dataset_table='{{ project }}.{{ dataset }}.{{ orig_table }}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND'
    )

    # TASK
    query = (
        '''
        SELECT
            `year`,
            `area`,
            ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM
            `{{ project }}.{{ dataset }}.{{ orig_table }}`
        GROUP BY
            `year`,
            `area`
        ORDER BY
            `area` ASC
        '''
    )

    tabla_resumen = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen',
        sql=query,
        destination_dataset_table='{{ project }}.{{ dataset }}.{{ dest_table }}',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-central1'
    )

    # TASK
    move_files = PythonOperator(
        task_id='move_files',
        python_callable=move_objects,
        op_kwargs={
            'source_bucket': '{{ bucket }}', 
            'destination_bucket': '{{ backup_bucket }}',
            'prefix': '{{ ts_nodash }}'
        },
        provide_context=True
    )

    end_task = DummyOperator(task_id='End')

# Dependencies
start_task >> list_files >> cargar_datos >> tabla_resumen >> move_files >> end_task
