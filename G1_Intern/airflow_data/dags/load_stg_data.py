""" This dag loads incremental data from sources to the staging layer of DWH. """

import pandas as pd
import logging
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timezone, timedelta
from pathlib import Path

from psycopg2.errors import UndefinedTable
from utils import CONFIG, TARGET_CONN_ID, TARGET_HOOK, \
    check_if_need_to_skip, get_ddl_from_conf, get_filepath, get_columns


default_args = {
    "owner": "rzv_de",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 29, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "catchup": False,
    "tags": ["rzv_de", "stg"],
}

@dag(default_args=default_args, description="ETL pipeline to load staging data from multiple sources", schedule_interval="*/3 * * * *", catchup=False)
def load_staging_data():
    
    @task()
    def prepare_tables():
        """ Creates stg table if not exists; truncates it otherwise. """
        
        for table in CONFIG["tables"]:
            sql = get_ddl_from_conf(table, "stg")
            print(sql, get_columns(table, "stg"))
            if sql: TARGET_HOOK.run(sql)
    
    
    @task()
    def extract(table:str, conn_id:str, **context) -> str:
        """ Selects increment of data from the source table and saves it to the csv file. Returns filepath. """
        
        increment_col = CONFIG["tables"][table]["load_params"]["increment_col"]        
        try:
            sql = f"""select max({increment_col}) from oda.{table};"""
            max_loaded_dttm = TARGET_HOOK.get_first(sql)[0]
        except UndefinedTable:
            max_loaded_dttm = None
            logging.info(f"First load of {table} table. Extract all data.")
        logging.info(f"Got max({increment_col}) from target {table} table: {max_loaded_dttm}")
        
        source_hook = PostgresHook(postgres_conn_id=conn_id)
        if not max_loaded_dttm:
            sql = f"""select * from {table};"""
        else:
            sql = f"""select * from {table} where {increment_col} > '{max_loaded_dttm}'::timestamp - interval '330 seconds';""" # to get all potential updated rows once in 5 min
        df = source_hook.get_pandas_df(sql)
        logging.info(f"Extracted increment of data from source [{conn_id}], {table} table with additional 330 seconds to handle updated rows.")
        
        execution_date = context["dag_run"].execution_date
        dag_id = context["dag"].dag_id
        filepath = get_filepath(dag_id, table, "extract", execution_date, "/tmp/airflow_staging", "csv", conn_id)
        
        df.to_csv(filepath, sep=";", header=True, index=False, mode="w", encoding="utf-8", errors="strict")
        logging.info(f"Data is extracted [{df.shape[0]} rows]: SOURCE {conn_id} from {table} table > {max_loaded_dttm} to {filepath}.")
        
        return filepath


    @task()
    def transform(filepath:str, table:str, conn_id:str, **context) -> str:
        """ Adds information about source (shop branch) and dag_run_id to identify what and when has loaded the data. Returns filepath. """
        
        df = pd.read_csv(filepath, sep=";", header=0, index_col=None, encoding="utf-8")
        df["src_id"] = CONFIG["source_conn_ids"][conn_id]["city_name"]
        df["dag_run_id"] = context["dag_run"].run_id
        
        execution_date = context["dag_run"].execution_date
        dag_id = context["dag"].dag_id
        filepath = get_filepath(dag_id, table, "transform", execution_date, "/tmp/airflow_staging", "csv", conn_id)
        
        df.to_csv(filepath, sep=";", header=True, index=False, mode="w", encoding="utf-8", errors="strict")
        logging.info(f"Data is transformed [{df.shape[0]} rows]: SOURCE {conn_id} from {table} table in {filepath}")

        return filepath


    @task
    def load(filepath:str, table:str, conn_id:str):
        """ Loads data to the staging layer of DWH. """
        
        df = pd.read_csv(filepath, sep=";", header=0, index_col=None, encoding="utf-8")
        
        tech_load_column = [pair for pair in CONFIG["tables"][table]["tech_load_column"].items()][0]
        df[tech_load_column[0]] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        df.to_sql(table,
            TARGET_HOOK.get_sqlalchemy_engine(),
            schema="stg",
            chunksize=1000,
            if_exists="append",
            index=False)
        logging.info(f"Data is loaded [{df.shape[0]} rows]: SOURCE {conn_id} into stg.{table} table from {filepath}")
        

    start_task = ShortCircuitOperator(
        task_id="dummy_dag_start",
        python_callable=check_if_need_to_skip
    )
     
    prepare_schema_task = PostgresOperator(
        task_id="prepare_schema",
        postgres_conn_id=TARGET_CONN_ID,
        sql="""create schema if not exists stg;"""
    )
    
    end_task = EmptyOperator(task_id="dummy_end")
    
    prepare_tables_task = prepare_tables()

    trigger_load_oda_data = TriggerDagRunOperator(
        task_id="trigger_load_oda_data",
        trigger_dag_id="load_oda_data",
        wait_for_completion=True,
        deferrable=True,
        retries=1
    )
    
    for conn_id in CONFIG["source_conn_ids"]:
        @task_group(group_id=conn_id)
        def conn_tg():
            inner_start_task = EmptyOperator(task_id="dummy_start")
            inner_end_task = EmptyOperator(task_id="dummy_end")
            
            for table in CONFIG["source_conn_ids"][conn_id]["tables"]:
                @task_group(group_id=table)
                def etl_tg():
                    filepath_e = extract(table, conn_id)
                    filepath_t = transform(filepath_e, table, conn_id)
                    load(filepath_t, table, conn_id)

                inner_start_task >> etl_tg() >> inner_end_task

        start_task >> prepare_schema_task >> prepare_tables_task >> conn_tg() >> end_task >> trigger_load_oda_data

dag = load_staging_data()