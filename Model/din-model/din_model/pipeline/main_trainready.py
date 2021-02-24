# Copyright 2020, Futurewei Technologies
#
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

import yaml
import argparse
import os
import timeit

from pyspark import SparkContext
from pyspark.sql import functions as fn
from pyspark.sql.functions import lit, col, udf, collect_list, concat_ws, first
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from din_model.pipeline.util import load_config, load_df, load_batch_config
from din_model.pipeline.util import write_to_table, add_index, print_batching_info


def generate_trainready(hive_context, batch_config, 
                        interval_time_in_seconds,
                        logs_table_name, trainready_table):
    def index_df_trainready(df):
        # normalized the adv_id values to the adv_id_index values.
        df = add_index(df, "uckey", "uckey_index", drop_column=False)
        df = add_index(df, "media", "media_index", drop_column=False)
        df = add_index(df, "media_category",
                       "media_category_index", drop_column=False)
        df = add_index(df, "net_type", "net_type_index", drop_column=False)
        df = add_index(df, "gender", "gender_index", drop_column=False)
        df = add_index(df, "age", "age_index", drop_column=False)
        df = add_index(df, "region_id", "region_id_index", drop_column=False)
        return df

    def group_batched_logs(logs):
        # group logs from uckey + interval_time + keyword.
        # group 1: group by uckey + interval_starting_time + keyword
        df = logs.groupBy('uckey', 'interval_starting_time', 'keyword_index').agg(
            first('keyword').alias('keyword'),
            fn.sum(col('is_click')).alias('kw_clicks_count'),
            fn.count(fn.when(col('is_click') == 0, 1).otherwise(
                0)).alias('kw_shows_count')
        )
        df = df.withColumn('kwi_clicks_count', concat_ws(
            ":", col('keyword_index'), col('kw_clicks_count')))
        df = df.withColumn('kwi_shows_count', concat_ws(
            ":", col('keyword_index'), col('kw_shows_count')))
        df = df.withColumn('kw_clicks_count', concat_ws(
            ":", col('keyword'), col('kw_clicks_count')))
        df = df.withColumn('kw_shows_count', concat_ws(
            ":", col('keyword'), col('kw_shows_count')))

        # group 2: group by uckey + interval_starting_time
        df = df.groupBy('uckey', 'interval_starting_time').agg(
            concat_ws(",", collect_list('keyword_index')).alias('kwi'),
            concat_ws(",", collect_list('kwi_clicks_count')
                      ).alias('kwi_click_counts'),
            concat_ws(",", collect_list('kwi_shows_count')
                      ).alias('kwi_show_counts'),
            concat_ws(",", collect_list('keyword')).alias('interval_keywords'),
            concat_ws(",", collect_list('kw_clicks_count')
                      ).alias('kw_click_counts'),
            concat_ws(",", collect_list('kw_shows_count')
                      ).alias('kw_show_counts')
        )
        return df

    def collect_trainready(df_trainready_batched_temp):
        # group 3: group by uckey with the temp batched uckey-interval rows.

        df = df_trainready_batched_temp

        # To improve performance, remove sorting and move it to each uckey.
        df = df.orderBy(
            [col('uckey'), col('interval_starting_time').desc()])

        df = df.groupBy('uckey').agg(
            collect_list('interval_starting_time').alias(
                'interval_starting_time'),
            collect_list('kwi').alias('keyword_indexes'),
            collect_list('kwi_click_counts').alias('keyword_indexes_click_counts'),
            collect_list('kwi_show_counts').alias('keyword_indexes_show_counts'),
            collect_list('interval_keywords').alias('keywords'),
            collect_list('kw_click_counts').alias('keywords_click_counts'),
            collect_list('kw_show_counts').alias('keywords_show_counts')
        )

        uckey_split_col = fn.split(df['uckey'], ',')

        df = df.withColumn('media', uckey_split_col.getItem(0))
        df = df.withColumn('media_category', uckey_split_col.getItem(1))
        df = df.withColumn('net_type', uckey_split_col.getItem(2))
        df = df.withColumn('gender', uckey_split_col.getItem(3))
        df = df.withColumn('age', uckey_split_col.getItem(4))
        df = df.withColumn('region_id', uckey_split_col.getItem(5))

        df = df.withColumn('gender', df['gender'].cast(IntegerType()))
        df = df.withColumn('age', df['age'].cast(IntegerType()))
        df = df.withColumn('region_id', df['region_id'].cast(IntegerType()))
        return df

    trainready_table_temp = trainready_table + '_temp'
    timer_start = timeit.default_timer()
    start_date, end_date, load_minutes = batch_config

    starting_time_sec = int(datetime.strptime(
        start_date, "%Y-%m-%d").strftime("%s"))
    ending_time_sec = int(datetime.strptime(
        end_date, "%Y-%m-%d").strftime("%s"))
    
    batched_round = 1
    while starting_time_sec < ending_time_sec:
        batched_time_end_sec = starting_time_sec + \
            timedelta(minutes=load_minutes).total_seconds()

        command = """select distinct interval_starting_time from {} 
                     where action_time_seconds between {} and {}"""
        intervals = hive_context.sql(command.format(logs_table_name,
                                     starting_time_sec, batched_time_end_sec)).collect()
        intervals = [
            _['interval_starting_time'] for _ in intervals]
        intervals.sort()
        command = """select * from {} where interval_starting_time between {} and {}"""
        start_time = intervals[0]
        end_time = intervals[-1]
        logs = hive_context.sql(
            command.format(logs_table_name, start_time, end_time))
        print_batching_info("Train ready", batched_round,
                            str(start_time), str(end_time))

        df_trainready = group_batched_logs(logs)
        mode = 'overwrite' if batched_round == 1 else 'append'
        write_to_table(df_trainready, trainready_table_temp, mode=mode)
        batched_round += 1

        # calculate new batched_time_end
        starting_time_sec = logs.filter('interval_starting_time == {}'.format(
            intervals[-1])).agg(fn.max('action_time_seconds')).take(1)[0][0]
        starting_time_sec += 1

    # load the batched trainready data and merge them with the same uckey.
    df_trainready_batched_temp = load_df(hive_context, trainready_table_temp)
    df_trainready = collect_trainready(df_trainready_batched_temp)
    df_trainready = index_df_trainready(df_trainready)
    write_to_table(df_trainready, trainready_table, mode='overwrite')
    timer_end = timeit.default_timer()
    print('Total batching seconds: ' + str(timer_end - timer_start))
    return df_trainready


def run(hive_context, cfg):
    cfg_logs = cfg['pipeline']['main_logs']
    logs_table_name = cfg_logs['logs_output_table_name']
    interval_time_in_seconds = cfg_logs['interval_time_in_seconds']

    cfg_train = cfg['pipeline']['main_trainready']
    trainready_table = cfg_train['trainready_output_table']
    
    batch_config = load_batch_config(cfg)

    generate_trainready(hive_context, batch_config, interval_time_in_seconds, 
                        logs_table_name, trainready_table)


if __name__ == "__main__":

    """
    This program performs the followings:
    adds normalized data by adding index of features
    groups data into time_intervals and ucdocs (labeled by uckey)
    """
    sc, hive_context, cfg = load_config(
        description="pre-processing train ready data")
    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
