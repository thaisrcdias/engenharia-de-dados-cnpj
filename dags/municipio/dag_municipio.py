import datetime
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from municipio.query_municipio import sql_municipio as query

project_id = models.Variable.get('project_id')
DEFAULT_DATAFLOW_LOCATION = 'us-east4-a'

default_args = {
    'start_date': days_ago(1),
    'retries': 3, 
    'retry_delay': datetime.timedelta(seconds=60),
    'dataflow_default_options': {
        'project': project_id,
        'location': DEFAULT_DATAFLOW_LOCATION,
        'temp_location': 'gs://tmp_cnpj_data/municipio',
        'staging-location': 'gs://tmp_cnpj_data/municipio',
	    'maxWorkers': 1
     }
}

with models.DAG(
    'load_municipio',
    default_args=default_args,
    schedule_interval=None,
    ) as dag:
    
    run_raw_municipio = DataflowTemplateOperator(
        task_id='run_raw_municipio',
        template='gs://dataflow-templates/latest/GCS_Text_to_BigQuery',
        parameters={
            'javascriptTextTransformFunctionName': 'transform_store_analysis',
            'JSONPath': 'gs://udf_cnpj_data/municipio/municipio_schema.json',
            'javascriptTextTransformGcsPath': 'gs://udf_cnpj_data/municipio/municipio_udf.js',
            'inputFilePattern': 'gs://a3datadesafio/municipio/F.K03200$Z.D10814.MUNICCSV',
            'outputTable': project_id + ':raw.municipio',
            'bigQueryLoadingTemporaryDirectory': 'gs://tmp_cnpj_data/municipio',
        }
    ),
    run_trusted_municipio = BigQueryOperator(
        task_id='run_trusted_municipio',
        sql=query,
        use_legacy_sql=False

    )

    run_raw_municipio >> run_trusted_municipio

