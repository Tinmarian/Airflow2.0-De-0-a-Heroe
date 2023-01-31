

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

from datetime import date


default_args = {
    'owner': 'David S',
    'start_date': days_ago(1) ### DAYS AGO (1)
}

dag_args = {
    'dag_id': '3_hooks_xcoms', #### CAMBIO NOMBRE
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args,
    'max_active_runs': 1
}


# HOOKS
def list_objects(bucket=None):
    return GCSHook().list(bucket)


# XCOMS
def move_objects(source_bucket=None, destination_bucket=None, prefix=None, **kwargs):

    storage_objects = kwargs['ti'].xcom_pull(task_ids='list_files')

    for ob in storage_objects:
        dest_ob = ob

        if prefix:
            dest_ob = f'{prefix}/{ob}'
        
        GCSHook().copy(source_bucket, ob, destination_bucket, dest_ob)
        GCSHook().delete(source_bucket, ob)


with DAG(**dag_args,tags=['Curso_1']) as dag:

    # LISTAR DOCUMENTOS
    list_files = PythonOperator(
        task_id='list_files',
        python_callable=list_objects,
        op_args=['original-bucked-987']
    )
    

    cargar_datos = GCSToBigQueryOperator(
        task_id='cargar_datos',
        bucket='original-bucked-987',
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=';',
        destination_project_dataset_table='regal-oasis-291423.working_dataset.retail_years',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND'
    )
    
    query = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `regal-oasis-291423.working_dataset.retail_years`
        GROUP BY `year`, `area`
        ORDER BY `area` ASC
        '''
    )
    
    tabla_resumen = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen',
        sql=query,
        destination_dataset_table='regal-oasis-291423.working_dataset.retail_years_resume',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-east1'
    )

    # MOVER DOCUMENTOS
    move_files = PythonOperator(
        task_id='move_files',
        python_callable=move_objects,
        op_kwargs={
            'source_bucket': 'original-bucked-987', 
            'destination_bucket': 'temp-bucket-987', 
            'prefix': str(date.today())
        }
    )


# DEPENDENCIES

list_files >> cargar_datos >> tabla_resumen >> move_files