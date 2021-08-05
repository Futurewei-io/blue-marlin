# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
#                                                 * "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from airflow import DAG
import datetime as dt
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta

default_args = {
    'owner': 'lookalike_application',
    'depends_on_past': False,
    'start_date': dt.datetime(2021, 7, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lookalike_application',
    default_args=default_args,
    schedule_interval=None,

)


def sparkOperator(
        file,
        task_id,
        **kwargs
):
    return SparkSubmitOperator(
        application='/home/airflow/airflow-apps/lookalike-model/lookalike_model/application/pipeline/{}'.format(file),
        application_args=['/home/airflow/airflow-apps/lookalike-model/lookalike_model/application/pipeline/config.yml'],
        conn_id='spark_default',
        executor_memory='8G',
        conf={'spark.driver.maxResultSize': '5g', 'spark.hadoop.hive.exec.dynamic.partition': True, 'spark.hadoop.hive.exec.dynamic.partition.mode': 'nonstrict'},
        driver_memory='8G',
        executor_cores=5,
        num_executors=20,
        task_id=task_id,
        dag=dag,
        **kwargs
    )



score_generator = sparkOperator('score_generator.py', 'lookalike_score_generator')
score_vector_table = sparkOperator('score_vector_table.py', 'lookalike_score_vector_table')
score_vector_rebucketing = sparkOperator('score_vector_rebucketing.py', 'lookalike_score_vector_rebucketing')
top_n_similarity_table_generator = sparkOperator('top_n_similarity_table_generator.py', 'lookalike_top_n_similarity_table_generator')


score_generator >> score_vector_table >> score_vector_rebucketing >> top_n_similarity_table_generator