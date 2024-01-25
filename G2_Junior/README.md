# Scenario

Configure incremental loading of tables products, customers, orders from sources pg-shop-1, pg-shop-2 ('New-York' and 'Istanbul' respectively) into non-historical DWH tables. Do not take into account that the data may be updated with some depth in the past. In this scenario it can't.

Create a config that specifies the table structure and its loading parameters. The code should be aware of possible changes to this config, adding new tables and sources.

Assume that all sources of all stores have the same table structure for each entity.

<details>
<summary>Hints</summary>
<br>

* First, you can load into staging layer the next batch of data that was created later than the last load max_dttm on the target. Then you can prepare datasets and insert rows with created_at > maximum loading time of the inserted rows.
* Provide idempotency whenever possible. Clear staging tables before inserting. At the same time, make sure that another DAG is not working with them right now.
* Filter the data, reducing as much as possible the number of rows in the dataset for the next download.
* Add technical fields to track the source, time of creation and/or updates of the data.
* Please note that the key for tables in the DWH will expand - it will become {table}_id and src_id.
</details>
<br>

Definition of done:
* 3 tables from two sources are loaded into DWH.
* The solution should be easily extensible for new tables and sources.
* There are no duplicates or gaps in the tables, IDs increase monotonically.
* Use ```airflow_data/dags/scripts/self-test.sql``` for self-testing.