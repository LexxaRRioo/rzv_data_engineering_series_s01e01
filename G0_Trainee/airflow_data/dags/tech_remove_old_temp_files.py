""" Removes temporary files that are older than 2 hours from local /tmp/airflow_staging directory. """

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    "owner": "rzv_de",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG(
    "tech_remove_old_temp_files",
    default_args=default_args,
    schedule="0 */2 * * *",  # Run every 2 hours
    tags=["maintenance"],
) as dag:

    # Define the bash command to remove old files
    bash_command = 'find /tmp/airflow_staging -name "*.csv" -type f -mmin +120 -delete'

    # Loop through all files in the directory and delete those that are older than 2 hours

    remove_old_files_task = BashOperator(
        task_id=f"remove_old_temp_files", bash_command=bash_command
    )

    remove_old_files_task
