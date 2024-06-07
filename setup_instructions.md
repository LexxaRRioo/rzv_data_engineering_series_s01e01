## Technical details

### Data generator

Reproduces the work of backend databases of stores selling exotic fruits with a simplified data model.

You can view and change the current settings here: `./deploy/env/shop-*.env` and `./docker/generator/config.json`. Don't forget to rebuild the generators while in `./deploy` directory: `docker compose up -d --build shop-1 shop-2`.

<details>
<summary>Generator features</summary>
<br>

* Using the Faker package, generates rows for tables in the Postgres database
* Multiple source instances can be deployed (simulating shop's branches)
* Reproducibility of generating and updating data through seed: `GENERATOR_SEED`
* Deleting data older than time X via `DELETE_OLDER_THAN_SEC`
* Update random number of columns in rows with managed depth in the past: `update_rows_per_tick, UPDATE_NOT_OLDER_THAN_SEC`
* Automatic stop after a specified time: `STOP_GENERATOR_AFTER_SEC`
* Managed frequency of data insertion and modification (time between ticks): `TICK_INTERVAL_SEC`
* Detailed logging of the DEBUG level and convenient INFO level in the container: `docker logs shop-1 -f` and in `./generator_app_data/logs`
</details>


### Database connectivity
The options are in `./deploy/env/`.
In Airflow, it is recommended to use Admin - Connections to manage connections.

I suggest studying the data schema on the sources directly from `dbeaver` after starting the generators and setting up the connections; documentation for the project is not always available.


### Deploying infrastructure locally
1. Fork the repository and clone it to your computer: `git clone https://github.com/%Username%/rzv_de_series_s01e01.git` .
2. Install Docker Desktop (I've tested on 4.26.1 (131620)). If you don't have a favorite IDE yet, install VS Code with the Python extension.
3. Open the repository in VS Code, go to the console `Ctrl+` ` and to the `deploy` directory in the selected grade `cd ./%Grade%/deploy`
4. Spin-up the services `docker compose up -d --build`. It will download images during first load, wait patiently. Airflow takes about a minute to load. In case if some functionality doesn't work, for example generator doesn't update data in Middle after Junior setup, rebuild images using  `docker compose up -d --build shop-1 shop-2`.
5. Go to the UI service pages and log in
* Airflow: `localhost:8080` ; airflow/airflow
* dbeaver: `localhost:80/#/admin` ; cbadmin/Password1 ; next return to the main page via logo in the upper left corner.
6. Copy to or edit the DAG in `./%Grade%/airflow_data/dags`, the changes will be updated in a couple of seconds, F5 is not required. Install the needed packages and modules by adding them to `./%Grade%/docker/airflow/requirements.txt` followed by rebuilding the airflow container (connections and variables should be set up again, see the section below) `docker compose up -d --build af-standalone`.
7. Set up connections in `dbeaver` via "+" - PostgreSQL. Don't forget to click "save credentials".
If a grade is above G0_Trainee, set up connections in `Airflow` via Admin - Connections, add variables in Admin - Variables, if necessary. If you find connections are lost after a pc/mac/docker restart, uncomment lines in `af-init` service inside `deploy/docker-compose.yml` and run `docker compose up -d` again. It worth to learn how to add them manually once.
8. Check that the generators are working by viewing the logs in `./%Grade%/generator_app_data/logs` or `docker logs shop-1 -f`, and run the DAGs via unpause. The data will flow into `pg-dwh` and will be available in `dbeaver`.