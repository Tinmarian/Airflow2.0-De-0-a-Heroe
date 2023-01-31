

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

######################### AZURE BLOBS to GCS
from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import AzureBlobStorageToGCSOperator


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

    #########################
    transferir_azure = AzureBlobStorageToGCSOperator(
        task_id='transferir_azure',
        # Azure
        wasb_conn_id='wasb_default', 
        container_name='airflowbucket',
        blob_name='retail_2020_azure.csv',
        file_path='retail_2020_azure.csv',
        # GCP
        gcp_conn_id='google_cloud_default',
        bucket_name='azure-bucket-987',
        object_name='retail_2020_azure.csv',
        filename='retail_2020_azure.csv',
        gzip=False,
        delegate_to=None
    )
    #########################

    cargar_datos = GCSToBigQueryOperator(
        task_id='cargar_datos',
        bucket='azure-bucket-987',  #########################
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=';',
        destination_project_dataset_table='regal-oasis-291423.working_dataset.retail_years_azure',  #########################
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND'
    )
    
    query = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `regal-oasis-291423.working_dataset.retail_years_azure`
        GROUP BY `year`, `area`
        ORDER BY `area` ASC
        '''
    )
    
    tabla_resumen = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen',
        sql=query,
        destination_dataset_table='regal-oasis-291423.working_dataset.retail_years_resume_azure',   #########################
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-east1'
    )


######################### DEPENDENCIES

transferir_azure >> cargar_datos >> tabla_resumen