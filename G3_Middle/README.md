# Scenario

Configure incremental loading of all tables from all sources into historical DWH tables with SCD2. Please note that the data may be updated with some depth in the past.

Set up logging.

<details>
<summary>Hints</summary>
<br>

* First, you can load into staging layer the next batch of data that was created later than the last load max_dttm on the target.  Then - prepare datasets and insert values for rows with created_at > max_dttm immediately, and for the remaining, potentially updated rows, compare hashes using concatenated non-key values.
* Provide idempotency whenever possible. Clear staging tables before inserting. At the same time, make sure that another dag is not working with them right now.
* Filter the data, reducing as much as possible the number of rows in the dataset for the next download.
* Add technical fields to track the source, time of creation and/or update of data. Use them to provide SCD2.
* To update eff_to_dttm changed rows, it is convenient to use UPDATE TABLE SET ... FROM.
* Use `generator_app_data/logs` or `docker logs shop-1 -f` to track changed rows and check the result by searching for records where PK are not unique (a third field is added to the PK -- `eff_from_dttm`).
* Use `../G1_Intern/airflow_data/dags/scripts/self-test.sql` for self-testing.
</details>
<br>

Definition of done:
* 3 tables from two sources are loaded into DWH.
* The solution should be easily extensible for new tables and sources.
* History in rows is stored accurate to the second (SCD2 in timestamp datatype).
* The last row changes within one PK are captured before the next data batch is extracted, intermediate ones are skipped -- this is a common problem with batch downloads, it is allowed.