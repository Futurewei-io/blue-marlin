#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from airflow import DAG
import datetime as dt
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta

default_args = {
    'owner': 'dlpm',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 9, 21),
    'retries': 0  # ,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dlpm_data_prep',
    default_args=default_args,
    schedule_interval=None,
)


def sparkOperator(
        file,
        task_id,
        **kwargs
):
    return SparkSubmitOperator(
        application='/home/airflow/airflow-apps/predictor-dl-model/predictor_dl_model/pipeline/{}'.format(
            file),
        application_args=[
            '/home/airflow/airflow-apps/predictor-dl-model/predictor_dl_model/config.yml'],
        conn_id='spark_default',
        conf={'spark.driver.maxResultSize': '8g'},
        driver_memory='16G',
        executor_cores=5,
        num_executors=10,
        executor_memory='16G',
        task_id=task_id,
        dag=dag,
        **kwargs
    )

show_config = sparkOperator('show_config.py', 'show_config')

main_filter_si_region_bucket = sparkOperator('main_filter_si_region_bucket.py', 'main_filter_si_region_bucket')

main_ts = sparkOperator('main_ts.py',
                        'main_ts',
                        py_files='/home/airflow/airflow-apps/predictor-dl-model/predictor_dl_model/pipeline/transform.py')

main_cluster = sparkOperator('main_cluster.py', 'main_cluster')

main_distribution = sparkOperator('main_distribution.py', 'main_distribution')

main_norm = sparkOperator('main_norm.py',
                          'main_norm',
                          py_files='/home/airflow/airflow-apps/predictor-dl-model/predictor_dl_model/pipeline/transform.py')

main_tfrecords = sparkOperator('main_tfrecords.py',
                               'main_tfrecords',
                               jars='/home/airflow/airflow-apps/predictor-dl-model/predictor_dl_model/spark-tensorflow-connector_2.11-1.15.0.jar')


show_config >> main_filter_si_region_bucket >> main_ts >> main_cluster >> main_distribution >> main_norm >> main_tfrecords

