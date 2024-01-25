# Scenario

Configure incremental loading of all tables from all sources into historical DWH tables with SCD2. Please note that the data may be updated with some depth in the past.

Set up logging and auto-tests, discard inconsistent data into an analogue of the dead letter queue, guided by the Write - Audit - Publish principle.

By limiting docker compose af-scheduler to 4 cores 4GB RAM, conduct “load testing” and determine the maximum average throughput in rows in 10 minutes that the authored solution can ingest. Let's say that up to 60% of ```insert_rows_per_tick``` could be updated per tick. Use generator parameters to change the speed at which data appears at the source.

<details>
<summary>Hints</summary>
<br>

* To avoid breaking the logic, keep the generator values comparison in the following order: STOP_GENERATOR_AFTER_SEC > DELETE_OLDER_THAN_SEC > UPDATE_NOT_OLDER_THAN_SEC > TICK_INTERVAL_SEC.
</details>
<br>

Definition of done:
* 3 tables from two sources are loaded into DWH.
* The solution should be easily extensible for new tables and sources.
* History in rows is stored accurate to the second (SCD2 via timestamp).
* The last row changes within one PK are captured before the next data batch is taken, intermediate ones are skipped -- this is a common problem with batch downloads, it is allowed.
* Delay in the appearance of data in DWH is up to 5 minutes with standard generator settings.
* For every FK in the fact table there is a record with corresponding PK in the dimension table, inconsistent data is stored in tables "on the side".
* The solution is optimized for heavy load; the average number of rows that the system can pass through in 10 minutes is known.

Bonus:
* If a record with an ID discarded at the time of Audit stage arrives with one of the following ingestions, you need to publish these discarded rows to the main table.