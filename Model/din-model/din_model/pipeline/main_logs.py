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

import yaml
import argparse
import timeit

from pyspark import SparkContext
from pyspark.sql import functions as fn
from pyspark.sql.functions import lit, col, udf, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from din_model.pipeline.util import load_config, load_batch_config
from din_model.pipeline.util import write_to_table, print_batching_info


def join_logs(hive_context, batch_config,
              interval_time_in_seconds, log_table_names):
    def union_logs(df_clicklog, df_showlog):
        # union click log and show log.
        columns = ['did', 'is_click', 'action_time', 'keyword', 
                   'keyword_index', 'media', 'media_category', 
                   'net_type', 'gender', 'age', 'adv_id']

        df_clicklog = df_clicklog.withColumn('is_click', lit(1))
        df_clicklog = df_clicklog.select(columns)

        df_showlog = df_showlog.withColumn('is_click', lit(0))
        df_showlog = df_showlog.select(columns)

        df_unionlog = df_showlog.union(df_clicklog)
        return df_unionlog

    def transform_action_time(df_logs, interval_time_in_seconds):
        _udf_time = udf(lambda x: int(datetime.strptime(
            x, '%Y-%m-%d %H:%M:%S.%f').strftime("%s")), IntegerType())
        df_logs = df_logs.withColumn(
            'action_time_seconds', _udf_time(col('action_time')))

        _udf_interval_time = udf(lambda x: x - x %
                                 interval_time_in_seconds, IntegerType())
        df_logs = df_logs.withColumn(
            'interval_starting_time', _udf_interval_time(col('action_time_seconds')))

        return df_logs

    timer_start = timeit.default_timer()
    start_date, end_date, load_minutes = batch_config
    starting_time = datetime.strptime(start_date, "%Y-%m-%d")
    ending_time = datetime.strptime(end_date, "%Y-%m-%d")
    showlog_table_name, clicklog_table_name, logs_table_name = log_table_names

    batched_round = 1
    while starting_time < ending_time:
        batched_time_start_str = starting_time.strftime("%Y-%m-%d %H:%M:%S")
        batched_time_end = starting_time + \
            timedelta(minutes=load_minutes)
        batched_time_end_str = batched_time_end.strftime("%Y-%m-%d %H:%M:%S")
        print_batching_info(
            "Main logs", batched_round, batched_time_start_str, batched_time_end_str)
        command = """select did, action_time, keyword, keyword_index, 
                     media, media_category, net_type, gender, 
                     age, adv_id from {} where action_time >= '{}' 
                     and action_time < '{}'"""
        df_clicklog_batched = hive_context.sql(command.format(
            clicklog_table_name, batched_time_start_str, batched_time_end_str))
        df_showlog_batched = hive_context.sql(command.format(
            showlog_table_name, batched_time_start_str, batched_time_end_str))
        df_logs_batched = union_logs(df_clicklog_batched, df_showlog_batched)
        df_logs_batched = transform_action_time(
            df_logs_batched, interval_time_in_seconds)
        df_logs_batched = df_logs_batched.withColumn('uckey',
                                                     concat_ws(",", col('media'), col('media_category'),
                                                     col('net_type'), col('gender'), col('age')))
        mode = 'overwrite' if batched_round == 1 else 'append'
        write_to_table(df_logs_batched, logs_table_name, mode=mode)
        batched_round += 1
        starting_time = batched_time_end

    timer_end = timeit.default_timer()
    print('Total batching seconds: ' + str(timer_end - timer_start))


def run(hive_context, cfg):
    # prepare parameters for processing batched logs.
    cfg_clean = cfg['pipeline']['main_clean']
    cfg_clean_output = cfg_clean['data_output']

    batch_config = load_batch_config(cfg)

    clicklog_table_name = cfg_clean_output['clicklog_output_table']
    showlog_table_name = cfg_clean_output['showlog_output_table']
    
    cfg_logs = cfg['pipeline']['main_logs']
    logs_table_name = cfg_logs['logs_output_table_name']
    interval_time_in_seconds = cfg_logs['interval_time_in_seconds']

    log_table_names = (showlog_table_name,
                       clicklog_table_name, logs_table_name)

    join_logs(hive_context, batch_config,
              interval_time_in_seconds, log_table_names)


if __name__ == "__main__":
    """
    This program performs the followings:
    unions show and click logs
    adds time_interval related data for batch processing
    adds uckey
    """
    sc, hive_context, cfg = load_config(description="pre-processing logs data")
    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
