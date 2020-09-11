from airflow import DAG
import datetime as dt
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta

default_args = {
    'owner': 'din_model',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 7, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'din_model_integration',
    default_args=default_args,
    schedule_interval=None,

)


def sparkOperator(
        file,
        task_id,
        **kwargs
):
    return SparkSubmitOperator(
        application='/home/airflow/airflow/din_model/pipeline/{}'.format(file),
        application_args=['/home/airflow/airflow/din_model/config.yml'],
        conn_id='spark_default',
        executor_memory='32G',
        conf={'spark.driver.maxResultSize': '4g'},
        driver_memory='32G',
        executor_cores=5,
        num_executors=20,
        task_id=task_id,
        dag=dag,
        **kwargs
    )


clean = sparkOperator('main_clean.py', 'din_clean')
logs = sparkOperator('main_logs.py', 'din_logs')
region_adding = sparkOperator('main_logs_with_regions.py', 'din_add_region')
trainready = sparkOperator('main_trainready.py', 'din_trainready')
# forecasting = sparkOperator(
#     'main_forecasting.py',
#     'din_forecasting',
#     py_files='/home/airflow/airflow/din_model/trainer/rest_client.py'
# )

tfrecords = sparkOperator(
    'main_tfrecords.py',
    'din_tfrecords',
    jars='/home/airflow/airflow/din_model/spark-tensorflow-connector_2.11-1.15.0.jar')

clean >> logs >> region_adding >> trainready >> tfrecords
