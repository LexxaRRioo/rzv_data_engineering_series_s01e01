import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

file_path = '/tmp/airflow_staging'

local_workflow = DAG(
    "remove_old_files",
    schedule_interval = "0 */4 * * *", # every 4 hours
    start_date = datetime(2023,1,29),
    catchup=False,
    max_active_runs=1
)

with local_workflow:
    remove_task = BashOperator(
        task_id = 'rm_files',
        bash_command = f'find {file_path} -type f -name "*.csv" -delete'
    )

remove_task