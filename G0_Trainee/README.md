# Scenario

There's nothing to change, just run and enjoy.

# Solution

Here everything has already been done up to the middle level, run it and it will work.

Look here if you get stuck in implementing your own solution.

## Explanation why I implemented the solution the way I did
Ask the remaining questions in the chat, I will add the answers here.

<details>
<summary>Why Pandas was chosen as an "ETL tool"</summary>
<br>

* This is a widely used package
* Helps you get used to the concept of DataFrame, the experience with which can then be reused with PySpark
* Decided to start with something that doesn't require another docker container (e.g. PySpark) so as not to bloat the episode
</details>
<br>

<details>
<summary>Why are some python functions included in utils, while others are not</summary>
<br>

* In utils there are functions that are used in different dags (imported several times)
* Also there are functions that will most likely be reused in the future
* Custom, “one-time” functions were left inside the .py file within a dag

</details>
<br>

<details>
<summary>Why save the results of intermediate calculations to a file (csv)</summary>
<br>

* Airflow, even with TaskFlow, direct data transfer between tasks should be avoided (because XCOM is used which relies on backend database and the solution wouldn't be scalable)
* To access data from another task from one task, you need to put the result in some kind of storage (serialize and save)
* Stages are divided into tasks to maintain scaling. The Transform and Load processes themselves can be much more complex and longer, so you shouldn’t combine them with Extract
* You can see the status of tasks and have the opportunity to execute them again (very convenient during debugging process and even after then) by making changes to the DAG and making Clear on the Task Instance

</details>
<br>

<details>
<summary>Which Best Practice I've used</summary>
<br>

Python:
* Moved repetitive operations into functions, and repeated functions into a separate imported package (DRY principle)
* Type hints for function parameters and its result
* Global variables are named in UPPERCASE, internal functions begin with "_"
* Specify the file encoding when writing and reading, and select the separator (sep) as a character that cannot appear in the data
* Long variable names replace most comments, the code strives to be "self-documenting"
* When handling errors, use as precise Exceptions as possible

Storage:
* Record time in UTC
* Adding technical fields for further operations with data
* Generate DDL for tables and create them in a separate step before inserting data
* Use fast and reliable idempotent loading into staging via truncate - insert
* Use incremental loading into target

Airflow:
* Specified doc-string at the beginning of every dag and almost every function
* Logging at debug and info levels
* Use tags to organize DAGs
* Form the name of the intermediate files so that a repeated launch will overwrite the previously created file for the task instance, and a new dag run will create a new file
* DAG does not start again until the previous one has completed (so as not to spoil staging)
* To organize tasks, a group task is used, starting and ending empty tasks
* Processing is divided into steps, almost all of which can be safely restarted (except for non-idempotent load, but this is debatable -- should it or shouldn't be idempotent without other steps)
* Where convenient, use operators suitable for this purpose instead of hooks and custom python code
* Using Variables, Connections

</details>
<br>

<details>
<summary>How can this solution be improved</summary>
<br>

* Generate DAG for each table separately, reduce coupling
* Import packages inside functions where they are used
* When retrieving data from a source, list columns separated by commas explicitly, do not use SELECT * FROM
* Move reading and writing from intermediate files into separate functions in order to abstract from the CSV format and be able to replace it in one place
* It would be better to update in batches rather than as a large query
* Configure even more parameters, such as "minimum and maximum possible time for eff_* columns", width of the window for possibly changed data

</details>