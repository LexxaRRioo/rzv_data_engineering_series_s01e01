# app.py
import psycopg2
from faker import Faker
from datetime import datetime, timedelta, timezone
import time
import json
import logging
import os

GENERATOR_SEED = int(os.environ["GENERATOR_SEED"])
STOP_GENERATOR_AFTER_SEC = int(os.environ["STOP_GENERATOR_AFTER_SEC"])
DELETE_OLDER_THAN_SEC = int(os.environ["DELETE_OLDER_THAN_SEC"])
UPDATE_NOT_OLDER_THAN_SEC = int(os.environ["UPDATE_NOT_OLDER_THAN_SEC"])
TICK_INTERVAL_SEC = int(os.environ["TICK_INTERVAL_SEC"])

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

logging.getLogger("faker").setLevel(logging.ERROR)

# Set up logging to INFO file
info_handler = logging.FileHandler("/app/logs/info.log", mode="w")
info_handler.setLevel(logging.INFO)
info_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
info_handler.setFormatter(info_formatter)

# Set up logging to DEBUG file
debug_handler = logging.FileHandler("/app/logs/debug.log", mode="w")
debug_handler.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
debug_handler.setFormatter(debug_formatter)

# Set up root logger
logger = logging.getLogger(__name__)
logger.addHandler(info_handler)
logger.addHandler(debug_handler)
logger.setLevel(logging.DEBUG)  # set root logger level to DEBUG

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


def initialize(tables, tech_fields, cur):
    sql = f"""CREATE EXTENSION IF NOT EXISTS pgcrypto;"""
    cur.execute(sql)
    logger.debug(sql)

    logger.info("Creating tables...")
    metadata = {
        table_name: table_schema["columns"]
        for table_name, table_schema in tables.items()
    }
    for table_name, columns in metadata.items():
        columns_list = [
            f"{name} {col_config['type']}" for name, col_config in columns.items()
        ]
        columns_list.extend(
            [f"{name} {col_config}" for name, col_config in tech_fields.items()]
        )

        sql = f"""DROP TABLE IF EXISTS "{table_name}";"""
        cur.execute(sql)
        logger.debug(sql)

        columns_str = ", ".join(columns_list)
        sql = f"""CREATE TABLE "{table_name}" ({table_name[:-1]}_id SERIAL PRIMARY KEY, {columns_str});"""
        cur.execute(sql)
        logger.debug(sql)


def generate_data(tables, tech_fields):
    logger.info("Generating data...")
    fake = Faker()
    data = {}

    for table_name, table_schema in tables.items():
        columns = table_schema["columns"]
        insert_rows_per_tick = table_schema["params"]["insert_rows_per_tick"]
        data[table_name] = []
        for _ in range(insert_rows_per_tick):
            row = {}
            for column_name, col_config in columns.items():
                if "params" in col_config:
                    row[column_name] = getattr(fake, col_config["faker"])(
                        **col_config["params"]
                    )
                else:
                    row[column_name] = getattr(fake, col_config["faker"])()
            for column_name in tech_fields:
                if column_name == "created_at":
                    row[column_name] = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S%z"
                    )

            data[table_name].append(row)
    return data


def delete_data(tables, delete_older_than_sec, cur):
    dttm_to_del = datetime.now(timezone.utc) - timedelta(seconds=delete_older_than_sec)
    for table_name in tables:
        sql = f"""DELETE FROM "{table_name}" WHERE created_at <= '{dttm_to_del}';"""
        cur.execute(sql)
        logger.debug(sql)
    logger.info(f"Deleted all rows older than {dttm_to_del}.")


def insert_data(data, cur):
    for table_name, rows in data.items():
        for row in rows:
            columns = ", ".join(row.keys())
            values = ", ".join(
                [
                    (
                        f"'{value}'"
                        if not isinstance(value, int) or not isinstance(value, float)
                        else str(value)
                    )
                    for value in row.values()
                ]
            )
            sql = f"""INSERT INTO "{table_name}" ({columns}) VALUES ({values});"""
            cur.execute(sql)
            logger.debug(sql)
        logger.info(f"Inserted {len(rows)} row(s) into {table_name} table.")


def update_data(tables, update_not_older_than_sec, cur):
    """updates random (still based on seed) column in the table with a new value
    in 'update_rows_per_tick' rows with created_at in 'last update_not_older_than_sec' seconds
    """

    fake = Faker()

    for table_name, table_config in tables.items():
        update_rows_per_tick = table_config["params"]["update_rows_per_tick"]

        # Get current time and time threshold for updating rows
        current_time = datetime.now(timezone.utc)
        dttm_to_upd = current_time - timedelta(seconds=update_not_older_than_sec)

        # Select random rows that meets the time threshold
        column_names = list(table_config["columns"].keys())
        sql = f"""SELECT {table_name[:-1]}_id FROM "{table_name}" WHERE created_at >= '{dttm_to_upd}' ORDER BY md5({table_name[:-1]}_id::text) LIMIT {update_rows_per_tick}"""
        cur.execute(sql)
        logger.debug(sql)
        rows = cur.fetchall()
        updated_columns = {}

        for id in rows:
            # Choose a random column to update
            col_names = fake.random_elements(
                elements=column_names,
                length=fake.random_int(min=1, max=len(column_names)),
                unique=True,
            )
            update_clauses = []
            for col_name in col_names:
                col_config = table_config["columns"][col_name]

                if "params" in col_config:
                    new_value = getattr(fake, col_config["faker"])(
                        **col_config["params"]
                    )
                else:
                    new_value = getattr(fake, col_config["faker"])()

                new_value = (
                    f"'{new_value}'"
                    if not isinstance(new_value, int)
                    or not isinstance(new_value, float)
                    else str(new_value)
                )
                update_clauses.append(f"{col_name} = {new_value}")
            updated_columns[id[0]] = col_names

            # Update the row with the new value
            sql = f"""UPDATE "{table_name}" SET {', '.join(update_clauses)} WHERE {table_name[:-1]}_id = {id[0]};"""
            cur.execute(sql)
            logger.debug(sql)
        logger.info(
            f"Updated {len(rows)} row(s) in {table_name} table: {updated_columns}."
        )


if __name__ == "__main__":
    with open("config.json", "r") as f:
        config = json.load(f)

    Faker.seed(GENERATOR_SEED)

    start_dttm = datetime.now(timezone.utc)
    now_dttm = datetime.now(timezone.utc)

    # auto-reconnect
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.set_session(autocommit=True)
            cur = conn.cursor()
        except psycopg2.OperationalError:
            time.sleep(2)
            continue
        break

    try:
        initialize(config["tables"], config["tech_fields"], cur)
        while now_dttm - start_dttm < timedelta(seconds=STOP_GENERATOR_AFTER_SEC):
            if DELETE_OLDER_THAN_SEC > 0:
                delete_data(config["tables"], DELETE_OLDER_THAN_SEC, cur)
            if UPDATE_NOT_OLDER_THAN_SEC > 0:
                update_data(config["tables"], UPDATE_NOT_OLDER_THAN_SEC, cur)
            data = generate_data(config["tables"], config["tech_fields"])
            insert_data(data, cur)

            time.sleep(TICK_INTERVAL_SEC)
            now_dttm = datetime.now(timezone.utc)
    finally:
        cur.close()
        conn.close()
