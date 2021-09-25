import datetime
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
import os
from tensorflow.python.lib.io import file_io

project_id = models.Variable.get('project_id')
DEFAULT_DATAFLOW_LOCATION = 'us-east4-a'

with file_io.FileIO('gs://us-east4-airflow-f0cf12f4-bucket/dags/empresa/query_empresa.sql', 'r') as conteudo:
    query = conteudo.read()


default_args = {
    'start_date': days_ago(1),
    'retries': 3, 
    'retry_delay': datetime.timedelta(seconds=60),
    'dataflow_default_options': {
        'project': project_id,
        'location': DEFAULT_DATAFLOW_LOCATION,
        'temp_location': 'gs://tmp_cnpj_data/empresa',
        'staging-location': 'gs://tmp_cnpj_data/empresa',
	    'maxWorkers': 4
     }
}

with models.DAG(
    'load_empresa',
    default_args=default_args,
    schedule_interval=None,
    ) as dag:
    
    run_raw_empresa = DataflowTemplateOperator(
        task_id='run_raw_empresa',
        template='gs://dataflow-templates/latest/GCS_Text_to_BigQuery',
        parameters={
            'javascriptTextTransformFunctionName': 'transform_store_analysis',
            'JSONPath': 'gs://udf_cnpj_data/empresa/empresa_schema.json',
            'javascriptTextTransformGcsPath': 'gs://udf_cnpj_data/empresa/empresa_udf.js',
            'inputFilePattern': 'gs://a3datadesafio/empresa/K3241.K03200Y*.D10814.EMPRECSV' ,
            'outputTable': project_id + ':raw.empresa',
            'bigQueryLoadingTemporaryDirectory': 'gs://tmp_cnpj_data/empresa',
        }
    ),
    run_trusted_empresa = BigQueryOperator(
        task_id='run_trusted_empresa',
        sql=query,
        use_legacy_sql=False

    )

    run_raw_empresa >> run_trusted_empresa
