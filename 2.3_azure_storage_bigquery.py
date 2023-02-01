

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

######################### AZURE BLOBS to GCS
from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import AzureBlobStorageToGCSOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'Rodrigo N',
    'start_date': days_ago(7)
}

dag_args = {
    'dag_id': '2.3_azure_storage_bigquery', #########################
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}


with DAG(**dag_args,tags=['Curso_1']) as dag:

    start_task = DummyOperator(task_id='start_task')

    #########################
    transferir_azure_2020 = AzureBlobStorageToGCSOperator(
        task_id='transferir_azure_2020',
        # Azure
        container_name='airflowbucket',
        blob_name='retail_2020.csv',
        file_path='retail_2020.csv',
        # GCP
        bucket_name='azure_airflow23',
        object_name='retail_2020_azure.csv',
        filename='retail_2020_azure.csv',
        gzip=False,
        delegate_to=None
    )

    transferir_azure_2021 = AzureBlobStorageToGCSOperator(
        task_id='transferir_azure_2021',
        # Azure
        container_name='airflowbucket',
        blob_name='retail_2021.csv',
        file_path='retail_2021.csv',
        # GCP
        bucket_name='azure_airflow23',
        object_name='retail_2021_azure.csv',
        filename='retail_2021_azure.csv',
        gzip=False,
        delegate_to=None
    )
    #########################

    cargar_datos = GCSToBigQueryOperator(
        task_id='cargar_datos',
        bucket='azure_airflow23',  #########################
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=';',
        destination_project_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years_azure',  #########################
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND'
    )
    
    query = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `serene-gradient-371719.airflow_trabajo.retail_years_azure`
        GROUP BY `year`, `area`
        ORDER BY `area` ASC
        '''
    )
    
    tabla_resumen = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen',
        sql=query,
        destination_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years_resume_azure',   #########################
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-central1'
    )

    end_task = DummyOperator(task_id='end_task')

######################### DEPENDENCIES

start_task >> [transferir_azure_2020,transferir_azure_2021] >> cargar_datos >> tabla_resumen >> end_task