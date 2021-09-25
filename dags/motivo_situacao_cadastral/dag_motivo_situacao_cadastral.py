import datetime
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
import os
from tensorflow.python.lib.io import file_io

project_id = models.Variable.get('project_id')
DEFAULT_DATAFLOW_LOCATION = 'us-east4-a'

with file_io.FileIO('gs://us-east4-airflow-f0cf12f4-bucket/dags/motivo_situacao_cadastral/query_motivo_situacao_cadastral.sql', 'r') as conteudo:
    query = conteudo.read()



default_args = {
    'start_date': days_ago(1),
    'retries': 3, 
    'retry_delay': datetime.timedelta(seconds=60),
    'dataflow_default_options': {
        'project': project_id,
        'location': DEFAULT_DATAFLOW_LOCATION,
        'temp_location': 'gs://tmp_cnpj_data/motivo_situacao_cadastral',
        'staging-location': 'gs://tmp_cnpj_data/motivo_situacao_cadastral',
	    'maxWorkers': 1
     }
}

with models.DAG(
    'load_motivo_situacao_cadastral',
    default_args=default_args,
    schedule_interval=None,
    ) as dag:
    
    run_raw_motivo_situacao_cadastral = DataflowTemplateOperator(
        task_id='run_raw_motivo_situacao_cadastral',
        template='gs://dataflow-templates/latest/GCS_Text_to_BigQuery',
        parameters={
            'javascriptTextTransformFunctionName': 'transform_store_analysis',
            'JSONPath': 'gs://udf_cnpj_data/motivo_situacao_cadastral/motivo_situacao_cadastral_schema.json',
            'javascriptTextTransformGcsPath': 'gs://udf_cnpj_data/motivo_situacao_cadastral/motivo_situacao_cadastral_udf.js',
            'inputFilePattern': 'gs://a3datadesafio/motivo_situacao_cadastral/F.K03200$Z.D10814.MOTICSV',
            'outputTable': project_id + ':raw.motivo_situacao_cadastral',
            'bigQueryLoadingTemporaryDirectory': 'gs://tmp_cnpj_data/motivo_situacao_cadastral',
        }
    ),
    run_trusted_motivo_situacao_cadastral = BigQueryOperator(
        task_id='run_trusted_motivo_situacao_cadastral',
        sql=query,
        use_legacy_sql=False

    )

    run_raw_motivo_situacao_cadastral >> run_trusted_motivo_situacao_cadastral