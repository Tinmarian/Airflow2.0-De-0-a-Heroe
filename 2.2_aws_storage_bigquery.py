

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

######################### AWS S3 to GCS
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'Rodrigo N',
    'start_date': days_ago(7)
}

dag_args = {
    'dag_id': '2.2_aws_storage_bigquery', #########################
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}


with DAG(**dag_args,tags=['Curso_1']) as dag:

    start_task = DummyOperator(task_id='start_task')

    #########################
    transferir_aws = S3ToGCSOperator(
        task_id='transferir_aws',
        bucket='airflow23-bucket',
        dest_gcs='gs://aws_airflow23',
        prefix='retail_',
        replace=False,
        aws_conn_id='aws_default',
        gcp_conn_id='google_cloud_default'
    )
    #########################

    cargar_datos = GCSToBigQueryOperator(
        task_id='cargar_datos',
        bucket='aws_airflow23',   #########################
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=';',
        destination_project_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years_aws', #########################
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND'
    )
    
    query = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `serene-gradient-371719.airflow_trabajo.retail_years_aws`
        GROUP BY `year`, `area`
        ORDER BY `area` ASC
        '''
    )
    
    tabla_resumen = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen',
        sql=query,
        destination_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years_resume_aws', #########################
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-central1'
    )

    end_task = DummyOperator(task_id='end_task')

######################### DEPENDENCIES

start_task >> transferir_aws >> cargar_datos >> tabla_resumen >> end_task