# rzv_de_series_s01e01

![Main cover](./images/s01e01.jpg)

## 📽️ Welcome to the course!
Here is an open episode of the rzv Data engineering series training course. Choose which series you will turn on tonight - one that will distract you from life, or one that will give you the opportunity to learn skills and create!

The course is going in a self-paced format, the infrastructure is deployed locally in Docker containers. I expect that you will look for materials to answer your questions on your own and discuss them in the [general chat](https://t.me/rzv_de_series). A solution is applied to the task at the Middle level. The tasks are divided into different difficulty levels. Start with where you feel most comfortable and work your way up. The higher the grade, the more abstract the problem statement -- it’s just like in life.

The skills you acquire during the course can be transferred to work practice almost effortlessly. And, unlike most courses, here you work with “live” data that is generated in real time (in a simplified manner). By the end of the first season of the series, you will be able to experience data engineering problems in practice and write the solutions yourself.

The further along the course, the more modules will be attached following the “business development”:
* data collection from a local API service
* building data marts and BI dashboards
* ETL tool migration from Pandas to Spark
* integration of Data Quality tools
* a lot more

p.s. Check the translated readme.md versions in the root of main directories: [\[ru\] is available](README_ru.md).

## 🥱 TL;DR
1. Fork the repo and clone it to the pc/mac
2. Install docker desktop
3. Follow the steps in [database connectivity](#database-connectivity) and [infrastracture setup](#deploying-infrastructure-locally)
4. Choose G0_Trainee to run the code and see how it works. Get through grades from G1 to G3 to master data ingestion with Airflow

## 🎬 What's on TV today
This is the first episode covering the features of incremental loading through Apache Airflow. In the process of performing tasks at the Middle and Senior level, you will encounter many difficulties that exist in real work practice. At the same time, even Junior and Intern tasks will introduce you to new concepts and gradually prepare you for more complex tasks.

I encourage you to first try to solve the problem yourself, and then look at my version.

You will learn:
* incremental data loading using Airflow
* basic ETL via Pandas
* work with relational databases via SQL and Python
* setting up connections to sources in Airflow and pgAdmin4
* running applications in containers via Docker Compose

## 🎞️ Cast
![Stack used](./images/image-6.png)

* Python 3.10
* Postgres 15 (DWH)
* Pandas 2.1.4 (ETL)
* Apache Airflow 2.7.3 (Orchestrator)
* pgAdmin4 (DBMS client)
* Docker

## 👨🏻‍🦲 👦🏻 🧔🏻 Scenarios and grades
Each level has its own directory. With each level I reduce the amount of ready-to-run code and raise the task's complexity. The contents of the directories are slightly different, but the infrastructure is ready for use everywhere. Detailed tasks are described in ```README.md``` of each grade. Choose yours and feel free to lower the level if necessary.

**Trainee**: All code has already been implemented for the middle grade problem. Just launch and explore it. Also you can find notes explaining why I've implemented the solution this way.

**Intern**: Extend the existing configuration so that the written DAG starts loading data from a new source and new tables. Write a simple dag for working with the file system to clean-up temporary files using BashOperator.

**Junior**: Write incremental loading without taking into account historical storage. The data on the source is not updated.

**Middle**: Write an incremental load into SCD2 tables. Please note that the data may be updated at the source.

**Senior**: Assignment as for Middle + setup Write-Audit-Publish pattern to ensure data quality and conduct load testing of the written solution.


## Technical details are inside the dropdown (open and follow the steps)


<details>
<summary>About the data generator, databases connectivity and deployment of local infrastructure</summary>

### Data generator

Reproduces the work of backend databases of stores selling exotic fruits with a simplified data model.

You can view and change the current settings here: ```./deploy/env/shop-*.env``` and ```./docker/generator/config.json```. Don't forget to rebuild the generators while in ```./deploy``` directory: ```docker compose up -d --build shop-1 shop-2```.

<details>
<summary>Generator features</summary>
<br>

* Using the Faker package, generates rows for tables in the Postgres database
* Multiple source instances can be deployed (simulating shop's branches)
* Reproducibility of generating and updating data through seed: ```GENERATOR_SEED```
* Deleting data older than time X via ```DELETE_OLDER_THAN_SEC```
* Update random number of columns in rows with managed depth in the past: ```update_rows_per_tick, UPDATE_NOT_OLDER_THAN_SEC```
* Automatic stop after a specified time: ```STOP_GENERATOR_AFTER_SEC```
* Managed frequency of data insertion and modification (time between ticks): ```TICK_INTERVAL_SEC```
* Detailed logging of the DEBUG level and convenient INFO level in the container: ```docker logs shop-1 -f``` and in ```./generator_app_data/logs```
</details>


### Database connectivity
<a name="database-connectivity"></a>

The options are in ```./deploy/env/```.
In Airflow, it is recommended to use Admin - Connections to manage connections.

I suggest studying the data schema on the sources directly from ```pgAdmin4``` after starting the generators and setting up the connections; documentation for the project is not always available.


### Deploying infrastructure locally
<a name="deploying-infrastructure-locally"></a>

1. Fork the repository and clone it to your computer: ```git clone https://github.com/%Username%/rzv_de_series_s01e01.git``` .
2. Install Docker Desktop (I've tested on 4.26.1 (131620)). If you don't have a favorite IDE yet, install VS Code with the Python extension.
3. Open the repository in VS Code, go to the console ```Ctrl+` ``` and to the ```deploy``` directory in the selected grade ```cd ./%Grade%/deploy```
4. Spin-up the services ```docker compose up -d```. Airflow takes about a minute to load. In case if some functionality doesn't work, for example generator doesn't update data in Middle after Junior setup, rebuild images using  ```docker compose up -d --build shop-1 shop-2```.
5. Go to the UI service pages and log in
* Airflow: ```localhost:8080``` ; airflow/airflow
* pgAdmin4: ```localhost:80``` ; admin@admin.com/root
6. Copy to or edit the DAG in ```./%Grade%/airflow_data/dags```, the changes will be updated in a couple of seconds, F5 is not required. Install the needed packages and modules by adding them to ```./%Grade%/docker/airflow/requirements.txt``` followed by rebuilding the airflow container (connections and variables should be set up again, see the section below) ```docker compose up -d --build af-scheduler```.
7. Set up a connection in ```Airflow``` via Admin - Connections, add variables in Admin - Variables, if necessary.
Set up a connection in ```pgAdmin4``` via Servers - Register - Server.
8. Check that the generators are working by viewing the logs in ```./%Grade%/generator_app_data/logs``` or ```docker logs shop-1 -f```, and run the DAGs via unpause. The data will flow into ```pg-dwh``` and will be available in ```pgAdmin4```.

</details>

## Gallery
Historical data storage with SCD2:
![Historical storage with SCD2](./images/image.png)

Incremental loading via Airflow:
![Incremental loading using airflow](./images/image-1.png)

Generator logs with varying levels of detail:
![INFO logs](./images/image-3.png)
![DEBUG logs](./images/image-4.png)

Completely local infrastructure with everything you need:
![Local infrastracture with all you've need](./images/image-5.png)


## 🚧 Identified but not yet resolved problems
* Episode requires 5-7 GB RAM for simultaneous operation of the entire infrastructure on Win and Mac (docker desktop is heavy + many services run). [It is recommended to increase virtual RAM/swap file](https://www.windowscentral.com/how-change-virtual-memory-size-windows-10).
* Each time after ```docker compose up``` airflow is re-initialized. The dags are saved, but ```Connections``` and ```Variables``` need to be filled-in again.


## 👷🏻 About the author
Aleksei Razvodov, Data engineer with 5+ years of experience in the industry. I strive to convey my understanding of the work of a data engineer and help those who are developing along this path.

If this repository helped you and you liked it, give it a ⭐ and subscribe to the social networks.
* [Mentor](https://razvodov-mentorship-de.notion.site/About-me-and-mentorship_ru-06510bfd4bbd4dcba93c351df0ff4a0e)
* [Telegram channel about Data Engineering](https://t.me/rzv_de)
* [LinkedIn about Data Engineering](https://www.linkedin.com/in/razvodov-alexey/)
* [CV](https://docs.google.com/document/d/1tYi0s7yNsGl_Xts5CrHDegLvAtlHtz7jPSp074MfCyI/edit?usp=sharing)
* [Contact me](https://t.me/razvodov_de_mentor)

<img src="images/photo.jpg" alt="Personal photo" width="300" style="display: block; margin: auto"/>
