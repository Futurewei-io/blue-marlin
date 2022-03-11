from airflow import DAG
import datetime as dt
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'performance_forcasting',
    'depends_on_past': False,
    'start_date': dt.datetime(2021, 3, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'inventory_updater',
    default_args=default_args,
    schedule_interval=None,

)


def sparkOperator(
        file,
        task_id,
        **kwargs
):
        return SparkSubmitOperator(
                application='/home/airflow/airflow-apps/inventory-updater/inventory_updater/{}'.format(file),
                application_args=['/home/airflow/airflow-apps/inventory-updater/conf/config.yml'],
                conn_id='spark_default',
                executor_memory='32G',
                conf={'spark.driver.maxResultSize': '8g'},
                driver_memory='16G',
                executor_cores=5,
                num_executors=10,
                task_id=task_id,
                dag=dag,
                **kwargs
        )

# show_inputs = BashOperator(
#     task_id='show_inputs',
#     bash_command='python /home/airflow/inventory-updater/inventory_updater/show_inputs.py /home/airflow/inventory-updater/conf/config.yml',
#     dag=dag)

show_inputs = sparkOperator('show_inputs.py', 'show_inputs')
inventory_up = sparkOperator('inventory_updater.py',
                             'inventory_updater',
                             jars='/home/airflow/airflow-apps/inventory-updater/lib/elasticsearch-hadoop-7.6.2.jar')
show_output = sparkOperator('show_output.py', 'show_output')

show_inputs >> inventory_up >> show_output