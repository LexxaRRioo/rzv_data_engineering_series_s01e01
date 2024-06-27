""" This dag loads incremental data from staging layer to the dds. """

import pandas as pd
import logging
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timezone, timedelta

from utils import (
    CONFIG,
    TARGET_CONN_ID,
    TARGET_HOOK,
    get_ddl_from_conf,
    hash_nonpk_cols_sha1_df,
    get_filepath,
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)

default_args = {
    "owner": "rzv_de",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 19, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "tags": ["rzv_de", "dds"],
}


def _prepare_update_sql(df: pd.DataFrame, table) -> str:
    """Based on dataframe to update makes a UPDATE FROM query. Returns sql.
    Implements naive approach, which works only if row's updated_dttm breaks
    the last (and only last for each PK) version of row into two rows."""

    pk_cols_without_scd2 = CONFIG["tables"][table]["pk_cols_without_scd2"]
    scd2_columns = [
        pair for pair in CONFIG["tables"][table]["load_params"]["scd2_columns"].items()
    ]
    tech_load_column = [
        pair for pair in CONFIG["tables"][table]["tech_load_column"].items()
    ][0]
    join_on_cols = [f"tgt.{col} = t.{col}" for col in pk_cols_without_scd2]

    cols = pk_cols_without_scd2.copy()
    cols.append(tech_load_column[0])
    cte_rows = []
    for i, row in df.iterrows():
        values = []

        # Wraps varchar, dates and other column values in "'" (value -> 'value')
        for col in cols:
            value = (
                f"'{row[col]}'"
                if not isinstance(row[col], int) and not isinstance(row[col], float)
                else str(row[col])
            )
            values.append(value)
        cte_rows.append(f"""({", ".join(values)})""")
        if i % 10 == 0:
            cte_rows.append("\n")
        cte_update_values = f", ".join(cte_rows).replace(", \n, ", ", \n")

    sql = f"""with temp as (select * from (values 
                    {cte_update_values}
                    ) as tab ({", ".join(cols)}))
update dds.{table} tgt
set {scd2_columns[1][0]} = t.{tech_load_column[0]}::{tech_load_column[1]} - interval '1 second'
from temp as t
where {' and '.join(join_on_cols)} and tgt.{scd2_columns[1][0]} = '9999-12-31 23:59:59'::{scd2_columns[1][1]};
"""
    return sql


@dag(
    default_args=default_args,
    description="ETL pipeline to load data from staging layer to dds layer",
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def load_dds_data():

    @task()
    def prepare_tables():
        for table in CONFIG["tables"]:
            sql = get_ddl_from_conf(table, "dds")
            if sql:
                TARGET_HOOK.run(sql)

    @task()
    def extract(table: str, **context) -> str:
        """Selects all data from staging table and saves it into the csv file. Returns filepath."""

        sql = f"""select * from stg.{table};"""
        logging.debug(sql)
        df = TARGET_HOOK.get_pandas_df(sql)
        logging.info(f"Extracted data from stg.{table} table.")

        execution_date = context["dag_run"].execution_date
        dag_id = context["dag"].dag_id
        filepath = get_filepath(
            dag_id, table, "extract", execution_date, "/tmp/airflow_staging", "csv"
        )

        df.to_csv(
            filepath,
            sep=";",
            header=True,
            index=False,
            mode="w",
            encoding="utf-8",
            errors="strict",
        )
        logging.info(
            f"Data is extracted [{df.shape[0]} rows]: from stg.{table} table and saved to {filepath}."
        )

        return str(filepath)

    @task()
    def transform(filepath, table, **context) -> list[str]:
        """Prepares datasets to load. Returns list of filepaths. [0]th path is for insert, [1]st is for update."""

        df = pd.read_csv(filepath, sep=";", header=0, index_col=None, encoding="utf-8")
        df["dag_run_id"] = context["dag_run"].run_id
        scd2_columns = [
            pair
            for pair in CONFIG["tables"][table]["load_params"]["scd2_columns"].items()
        ]

        # Prepare to divide loaded part to pure increment (insert) and those where rows could be updated
        increment_col = CONFIG["tables"][table]["load_params"]["increment_col"]
        sql = f"""select max({increment_col}) from dds.{table};"""
        logging.debug(sql)
        max_loaded_dttm = TARGET_HOOK.get_first(sql)[0]
        df_i = df.copy(deep=True)
        if max_loaded_dttm:
            df_i = df_i[pd.to_datetime(df_i[increment_col]) > max_loaded_dttm]
            df_u_dataset = df.copy(deep=True)
            df_u_dataset = df_u_dataset[
                pd.to_datetime(df_u_dataset[increment_col]) <= max_loaded_dttm
            ]

        # Insert part
        df_i[scd2_columns[0][0]] = df_i[increment_col]
        df_i[scd2_columns[1][0]] = datetime(9999, 12, 31, 23, 59, 59)

        execution_date = context["dag_run"].execution_date
        dag_id = context["dag"].dag_id
        filepath_i = get_filepath(
            dag_id, table, "transform_i", execution_date, "/tmp/airflow_staging", "csv"
        )

        df_i.to_csv(
            filepath_i,
            sep=";",
            header=True,
            index=False,
            mode="w",
            encoding="utf-8",
            errors="strict",
        )
        logging.info(
            f"Data is transformed (insert) [{df_i.shape[0]} rows]: ready to load from stg.{table} table in {filepath}"
        )

        # Update part, only if there's already loaded data in target
        filepath_u = None

        if max_loaded_dttm:
            sql = f"""select * from dds.{table} 
where {increment_col} between '{max_loaded_dttm}'::timestamp - interval '330 seconds' and '{max_loaded_dttm}' 
and {scd2_columns[1][0]} = '9999-12-31 23:59:59';"""
            logging.debug(sql)
            df_u_target = TARGET_HOOK.get_pandas_df(sql)

            df_u_target_hashed = hash_nonpk_cols_sha1_df(df_u_target, table)
            df_u_dataset_hashed = hash_nonpk_cols_sha1_df(df_u_dataset, table)

            pk_cols_without_scd2 = CONFIG["tables"][table]["pk_cols_without_scd2"]

            df_u_temp = pd.merge(
                df_u_dataset_hashed,
                df_u_target_hashed,
                how="inner",
                on=pk_cols_without_scd2,
            )
            df_u_changed_rows = df_u_temp[
                df_u_temp["hashed_x"] != df_u_temp["hashed_y"]
            ]
            df_u_changed_rows = df_u_changed_rows.drop(columns=["hashed_x", "hashed_y"])

            tech_load_column = [
                pair for pair in CONFIG["tables"][table]["tech_load_column"].items()
            ][0]

            if df_u_changed_rows.shape[0] > 0:
                df_u = pd.merge(
                    df_u_dataset,
                    df_u_changed_rows,
                    how="inner",
                    on=pk_cols_without_scd2,
                )  # filter, save only updated rows
                df_u[tech_load_column[0]] = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                df_u[scd2_columns[0][0]] = df_u[tech_load_column[0]]
                df_u[scd2_columns[1][0]] = datetime(9999, 12, 31, 23, 59, 59)
                df_u["dag_run_id"] = context["dag_run"].run_id
                filepath_u = get_filepath(
                    dag_id,
                    table,
                    "transform_u",
                    execution_date,
                    "/tmp/airflow_staging",
                    "csv",
                )
                df_u.to_csv(
                    filepath_u,
                    sep=";",
                    header=True,
                    index=False,
                    mode="w",
                    encoding="utf-8",
                    errors="strict",
                )
                logging.info(
                    f"Data is transformed (update) [{df_u.shape[0]} rows]: ready to load from stg.{table} table in {filepath}"
                )
            else:
                logging.info(
                    f"There's no data to update in dds.{table} table from {filepath}"
                )

        return (filepath_i, filepath_u)

    @task
    def load(filepaths: list, table: str) -> None:
        """Loads data into target table with SCD2 history."""

        tech_load_column = [
            pair for pair in CONFIG["tables"][table]["tech_load_column"].items()
        ][0]

        # Update part, a little easier for DBMS to run updates before inserts
        if filepaths[1]:
            df_u = pd.read_csv(
                filepaths[1], sep=";", header=0, index_col=None, encoding="utf-8"
            )

            sql = _prepare_update_sql(df_u, table)
            logging.debug(sql)
            TARGET_HOOK.run(sql)
            logging.info(
                f"Old versions of rows from update part are updated [{df_u.shape[0]} rows]: into dds.{table} table from {filepaths[1]}"
            )

            df_u.to_sql(
                table,
                TARGET_HOOK.get_sqlalchemy_engine(),
                schema="dds",
                chunksize=1000,
                if_exists="append",
                index=False,
            )
            logging.info(
                f"New versions of rows from update part are loaded [{df_u.shape[0]} rows]: into dds.{table} table from {filepaths[1]}"
            )
        else:
            logging.info(
                f"There's no data to update in dds.{table} table from {filepaths[1]}"
            )

        # Insert part
        if filepaths[0]:
            df_i = pd.read_csv(
                filepaths[0], sep=";", header=0, index_col=None, encoding="utf-8"
            )
            df_i[tech_load_column[0]] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            df_i.to_sql(
                table,
                TARGET_HOOK.get_sqlalchemy_engine(),
                schema="dds",
                chunksize=1000,
                if_exists="append",
                index=False,
            )
            logging.info(
                f"Data is loaded [{df_i.shape[0]} rows]: into dds.{table} table from {filepaths[0]}"
            )
        else:
            logging.info(
                f"There's no data to load into dds.{table} table from {filepaths[0]}"
            )

    prepare_schema_task = PostgresOperator(
        task_id="prepare_schema",
        postgres_conn_id=TARGET_CONN_ID,
        sql="""create schema if not exists dds;""",
    )

    prepare_tables_task = prepare_tables()

    end_task = EmptyOperator(task_id="dummy_end")

    for table in CONFIG["tables"]:

        @task_group(group_id=table)
        def etl_tg():
            filepath_e = extract(table)
            filepath_t = transform(filepath_e, table)
            load(filepath_t, table)

        prepare_schema_task >> prepare_tables_task >> etl_tg() >> end_task


dag = load_dds_data()
