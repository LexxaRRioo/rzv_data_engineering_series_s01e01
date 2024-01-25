import pendulum
import pandas as pd
import hashlib
import logging

from airflow.models import Variable, DagRun as DR
from airflow.utils.state import State
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import settings

from psycopg2.errors import UndefinedTable
from pathlib import Path


logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')


CONFIG = Variable.get("load_data_conf", deserialize_json=True)
TARGET_CONN_ID = "dwh_conn"
TARGET_HOOK = PostgresHook(postgres_conn_id=TARGET_CONN_ID)


def get_columns(table:str, schema:str) -> list[str]:
        """ Returns list of "column datatype" definitions. """
        
        root_table_config = CONFIG["tables"][table]
        columns = [f"""{col} {root_table_config["columns"][col]}""" for col in root_table_config["columns"]]
        columns.extend([f"""{col} {root_table_config["tech_columns"][col]}""" for col in root_table_config["tech_columns"]])
        print(f"\n\n columns\n\n")
        if schema == "oda":
            is_scd2 = root_table_config["load_params"]["scd2"]
            scd2_columns = [pair for pair in root_table_config["load_params"]["scd2_columns"].items()]
            
            if is_scd2:
                columns.extend([f"{scd2_columns[0][0]} {scd2_columns[0][1]}", f"{scd2_columns[1][0]} {scd2_columns[1][1]}"])
        logging.debug(f"columns are: {columns}")
        
        return columns


def get_ddl_from_conf(table:str, schema:str) -> str:
    """ Returns ddl to either create a table or truncate it. """
    
    if _check_if_table_exists(table, schema) and schema == "stg":
        sql = f"""truncate stg.{table};"""
        
    elif _check_if_table_exists(table, schema):
        sql = None
        
    else:
        columns = get_columns(table, schema)
        sql = f"""create table {schema}.{table} ({", ".join(columns)});"""
        
    return sql


def _check_if_table_exists(table:str, schema:str) -> bool:
    try:
        sql = f"""select 1 from {schema}.{table} limit 1;"""
        TARGET_HOOK.get_first(sql)
        
        return True
    
    except UndefinedTable:
        
        return False
    
    
def check_if_need_to_skip(**context):
    """ Checks if there are no running instances of the dag; skip otherwise """

    task = context['ti'].task
    local_tz = pendulum.timezone('UTC')
    exec_dt = local_tz.convert(context['execution_date'])
    session = settings.Session()

    dags = session.query(DR).filter(
        DR.dag_id == task.dag_id,
        DR.execution_date != exec_dt,
        DR.state == State.RUNNING
    )

    dags = [d for d in dags]
    if len(dags) > 0:
        task.log.info(f"There are {len(dags)} running dags: {dags}")

    return len(dags) == 0


def hash_nonpk_cols_sha1_df(df: pd.DataFrame, table:str) -> pd.DataFrame:
    """Returns a pandas dataframe with primary key columns and a hashed column of concatenated non-primary key columns."""
        
    pk_cols = CONFIG["tables"][table]["pk_cols_without_scd2"]
    tech_columns = CONFIG["tables"][table]["tech_columns"]
    scd2_columns = [pair[0] for pair in CONFIG["tables"][table]["load_params"]["scd2_columns"].items()]
    
    nonpk_cols = [col for col in df.columns if col not in pk_cols \
        and col not in tech_columns \
        and col not in scd2_columns]
        
    res = df.copy(deep=True)
    res["hashed"] = res[nonpk_cols].apply(lambda row: hashlib.sha1("|".join(row.values.astype(str)).encode()).hexdigest(), axis=1)
    logging.debug(res.head(20), res.columns)
    
    return res[pk_cols + ["hashed"]]


def get_filepath(dag_id:str, table:str, etl_stage:str, execution_date:str, abs_path_dir:str, format:str, conn_id:str="") -> str:
    if len(conn_id) > 0: conn_id = conn_id + "_"
    
    output_file = f"""{dag_id}_{etl_stage}_{table}_{conn_id}{execution_date.strftime("%Y-%m-%dT%H-%M-%S")}.{format}"""
    output_dir = Path(abs_path_dir + f"""/{dag_id}/{execution_date.strftime("%Y-%m-%dT%H-%M-%S")}""")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    return str(Path(output_dir / output_file))