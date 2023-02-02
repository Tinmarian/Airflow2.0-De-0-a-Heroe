

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'Rodrigo N',
    'start_date': days_ago(7)
}

dag_args = {
    'dag_id': '2_simple_storage_bigquery',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}


with DAG(**dag_args,tags=['Curso_1']) as dag:

    start_task = DummyOperator(task_id='start_task')

    cargar_datos = GCSToBigQueryOperator(
        task_id='cargar_datos',
        bucket='airflow23_bucket',
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=';',
        destination_project_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND'
    )
    
    query = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `serene-gradient-371719.airflow_trabajo.retail_years`
        GROUP BY `year`, `area`
        ORDER BY `area` ASC
        '''
    )

    query_2020 = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `serene-gradient-371719.airflow_trabajo.retail_years` WHERE `year`=2020
        GROUP BY `year`,`area`
        ORDER BY `avg_income` ASC
        '''
    )

    query_2021 = (
        '''
        SELECT `year`, `area`, ROUND(AVG(`total_inc`), 4) AS avg_income
        FROM `serene-gradient-371719.airflow_trabajo.retail_years` WHERE year=2021
        GROUP BY `year`,`area`
        ORDER BY avg_income ASC
        '''
    )
    
    tabla_resumen_total = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen_total',
        sql=query,
        destination_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years_total_resume_WSL',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-central1'
    )

    tabla_resumen_2020 = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen_2020',
        sql=query_2020,
        destination_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years_resume_2020',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-central1'
    )

    tabla_resumen_2021 = BigQueryExecuteQueryOperator(
        task_id='tabla_resumen_2021',
        sql=query_2021,
        destination_dataset_table='serene-gradient-371719.airflow_trabajo.retail_years_resume_2021',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-central1'
    )

    end_task = DummyOperator(task_id='end_task')

# DEPENDENCIES

start_task >> cargar_datos >> [tabla_resumen_total,tabla_resumen_2020,tabla_resumen_2021] >> end_task