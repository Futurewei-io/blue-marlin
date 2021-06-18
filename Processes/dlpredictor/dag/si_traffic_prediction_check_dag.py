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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'si_traffic_orediction_check',
    'depends_on_past': False,
    'start_date': dt.datetime(2021, 3, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'si_traffic_orediction_check',
    default_args=default_args,
    schedule_interval=None,

)

def sparkOperator(
        file,
        task_id,
        executor_cores=5,
        num_executors=10,
        **kwargs
):
        return SparkSubmitOperator(
                application='/home/airflow/airflow-apps/dlpredictor/{}'.format(file),
                application_args=[],
                conn_id='spark_default',
                executor_memory='32G',
                conf={'spark.driver.maxResultSize': '8g'},
                driver_memory='32G',
                executor_cores=executor_cores,
                num_executors=num_executors,
                task_id=task_id,
                dag=dag,
                **kwargs
        )


si_traffic_orediction_check = sparkOperator('tests/si_traffic_prediction_ckeck/si_traffic_prediction_check.py',
        'si_traffic_orediction_check',
	py_files='/home/airflow/airflow-apps/dlpredictor/dist/dlpredictor-1.6.0-py2.7.egg,/home/airflow/airflow-apps/dlpredictor/lib/imscommon-2.0.0-py2.7.egg,/home/airflow/airflow-apps/dlpredictor/lib/predictor_dl_model-1.6.0-py2.7.egg')

si_traffic_orediction_check