import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

# main workflow dag
with DAG(
    dag_id="hello-world",
    schedule=None,
    start_date=dt.datetime(year=2022, month=10, day=27),
    end_date=None,
    tags=["learning", "example"],
):

    def print_hello(**context):
        return "Hello World!"

    hello_operator = PythonOperator(
        task_id="hello-operator", python_callable=print_hello
    )

hello_operator
