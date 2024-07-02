import datetime as dt
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'azhi',  # Владелец задач
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 6, 28, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
    'retries': 1
}

with DAG(
    dag_id="del_old_csv_staging_data",
    default_args= default_args,
    schedule_interval=timedelta(hours=3),
    catchup = False
) as dag:
    del_old_scv = BashOperator(
        task_id = "del_csv",
        bash_command = "find /tmp/airflow_staging/load_staging_data -name '*.csv' -type f -mmin +120 -delete"
    )

    del_old_scv