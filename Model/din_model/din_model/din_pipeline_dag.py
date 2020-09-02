from airflow import DAG
import datetime as dt
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta

default_args = {
    'owner': 'din_model',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 7, 22),
    'email': ['wyu@futurewei.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'din_model_integration',
    default_args=default_args,
    schedule_interval=None,

)

clean = SparkSubmitOperator(
        application='/home/wei2/airflow/din_model/pipeline/main_clean.py',
        application_args=['/home/wei2/airflow/din_model/config.yml'],
        conn_id='spark_default',
        executor_memory='16G',
        driver_memory='16G',
        executor_cores=5,
        num_executors=20,
        task_id='din_clean',
        dag=dag,
)


process = SparkSubmitOperator(
        application='/home/wei2/airflow/din_model/pipeline/main_processing.py',
        application_args=['/home/wei2/airflow/din_model/config.yml'],
        conn_id='spark_default',
        executor_memory='16G',
        driver_memory='16G',
        executor_cores=5,
        num_executors=20,
        task_id='din_processing',
        dag=dag,
)


tfrecords = SparkSubmitOperator(
        application='/home/wei2/airflow/din_model/pipeline/main_tfrecords.py',
        application_args=['/home/wei2/airflow/din_model/config.yml'],
        conn_id='spark_default',
        executor_memory='16G',
        driver_memory='16G',
        executor_cores=5,
        num_executors=20,
        jars='/home/wei2/airflow/din_model/spark-tensorflow-connector_2.11-1.15.0.jar',
        task_id='din_tfrecords',
        dag=dag,
)

clean >> process >> tfrecords
