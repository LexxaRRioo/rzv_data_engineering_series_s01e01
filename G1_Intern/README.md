# Scenario

Similar to ```oda.orders```, add loading of the ```oda.customers``` and ```oda.products``` tables. Then add a download from the pg-shop-2 source, similar to shop_1_conn.
You also need to author an Airflow DAG that will remove intermediate files from the directory in airflow.

To load tables, you need to deploy the environment, configure connections in Airflow - Admin - Connections and make changes to the config ```airflow_data/dags/scripts/airflow_variable.json```, which then needs to be imported into Admin - Variables.

<details>
<summary>Hints</summary>

* For city name look here: ```deploy/env```
* Technical fields and everything except ```columns```, fill in the same way as orders.
* Look at the column structure (data schema of the two missing tables) through pgAdmin by connecting to the sources.
* You can determine "too-long-lived" files using the bash command ```find``` with parameters.
* Learn JSON data types and how to differentiate between arrays and objects.
* Examine where connections and tables are used in the code to properly populate the configuration file. Use Ctrl+Shift+F to search in the entire repository (if you use VS Code).
* Comment out a section of code, save and run the dag to understand what will break and what it is responsible for.
</details>
<br>

Definition of Done:
* oda.products and oda.customers have appeared among the tables in dwh.
* All three tables are filled with data from both sources.
* Files in the ```cd /tmp/airflow_staging``` directory in the ```docker exec -it af-scheduler bash``` container are deleted at least once a day using a new dag.
* Use ```airflow_data/dags/scripts/self-test.sql``` for self-testing.