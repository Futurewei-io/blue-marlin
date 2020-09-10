### AIRFLOW
Airflow is a data pipeline tool that can integrate projects with multiple tasks, and monitor schedulers and logs for each task. 

### DAG
DAG, (Directed Acyclic Graph), is written in a Python script, which represents the DAG structure, tasks and its dependencies.
A typical DAG has 5 sessions: package importing, default_args, dag definition, tasks, and procedures of tasks. 

### TASK
A DAG can include multiple number of tasks. Each Python file that can run independently can be a task. One Python file can be re-used in multiple tasks. A task shall define the type of operator, connection id, task id and the dag it is assigned to. Optional configurations also include maximum number of driver memory, number of executors etc. While running a DAG, each task has its own log. 

### OPERATOR
DAG defines the type for operator for each task. A few most common operators include PythonOperator, BashOperator and SparkSubmitOperator. All the operator types and parameters can be found here: https://airflow.apache.org/docs/stable/_api/airflow/operators/index.html


### PREPARATION
Apache Airflow shall be installed on the working server, where the server can run spark-submit and has connection to database. The below command can be easily used to start Airflow webserver and scheduler:

    airflow webserver -p 8080
    airflow scheduler

### DIN-Model Pre-Processing EXECUTION
 
To run DIN-Model pre-processing on Airflow, follow the below steps.

1.	Download the code from GitHub (under Device Cloud Service /blue-marlin-model/din_model/din_model/pipeline). Securely copy the second last ‘din_model’ folder to the AIRFLOW_HOME directory. The DAG Python files are all under /blue-marlin-models/din_model/din_model/dags directory. Securely copy din_pipeline_dag.py to Airflow dag directory (airflow/dags). You can check if the DAG is workable by typing “python din_pipeline_dag.py” in the command line. If the DAG is workable, this command should not return anything.

2.	Launch the Airflow webserver and open the UI dashboard to check if this dag exists on the list of dags without showing any error on top. Then enable the dag.

3.	Click “Trigger Dag” icon in the rightest column named “Links”, then click “trigger”. 

4.	Go to “Browse Task Instances” to watch the status of this DAG run instance. Click each task id to view its status and logs. 


### CONCURRENCY SITUIATION
max_active_runs can be set within a DAG. This represents maximum number of active DAG-runs, beyond this number, the scheduler does not create new active DAG-runs.
To schedule the maximum number of task instances within a DAG, set concurrency (int) in the DAG, which determines the number of task instances allowed to run concurrently.
There is an issue of tasks overlapping between DAG-runs. This can cause tasks to compete for resources as well as duplicating or overwriting what the other task is doing. 

To prevent overlapping between DAG-runs:
1.	Set max_active_runs for a DAG to 1 to ensure that only one DAG-run is active at a time. In this case, only one DAG-run is executed at a time.
2.	Set depend_on_past to True such that a task does not execute unless the previous one completes.
3.	Finally, make the DAG use a pool with one slot.


### Error Handling
Each task inside a DAG has its own log record. Besides, we can also set up an independent task to handle the failed tasks or operations. 

### Add-ons
Apache Airflow supports DAGs to be run from another server, by installing Airflow Celery. Please refer to https://airflow.apache.org/docs/stable/executor/celery.html for more details.


