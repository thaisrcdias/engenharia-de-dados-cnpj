import datetime
from airflow import models
from airflow.operators import bash_operator
from airflow.utils.dates import days_ago
import os


project_id = models.Variable.get('project_id')
DEFAULT_DATAFLOW_LOCATION = 'us-east4-a'




default_args = {
    'start_date':  '2021-09-24',
    'schedule_interval': '0 0 10 * *',
    'retries': 3, 
    'retry_delay': datetime.timedelta(seconds=60),
    'depends_on_past': False,
    'çatchup' : False
     }


with models.DAG(
    'collect_data',
    default_args=default_args,
    schedule_interval=None,
    ) as dag:
    
    call_lambda = bash_operator.BashOperator(
        task_id='call_lambda',
        bash_command='pwd'
        #bash_command='curl {0})'.format(models.Variable.get('lambda_endpoint'))


    )

    call_lambda 