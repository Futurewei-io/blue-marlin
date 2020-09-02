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
from util import load_df, write_to_table, add_index


def load_and_group_batched_logs(hive_context, starting_date, ending_date, load_logs_in_minutes, interval_time_in_seconds, logs_table_name, trainready_table_name):
    def index_df_trainready(df):
        # normalized the adv_id values to the adv_id_index values.
        # the din model prefers to index values which have better values distribution.
        df = add_index(df, "uckey", "uckey_index", drop_column=False)
        df = add_index(df, "media", "media_index", drop_column=False)
        df = add_index(df, "media_category", "media_category_index", drop_column=False)
        df = add_index(df, "net_type", "net_type_index", drop_column=False)
        df = add_index(df, "gender", "gender_index", drop_column=False)
        df = add_index(df, "age", "age_index", drop_column=False)
        df = add_index(df, "region_id", "region_id_index", drop_column=False)
        return df

    def group_batched_logs(df_logs_batched):
        # group the logs to generate the train ready data from the basic unit of uckey + interval_time + keyword.
        # group 1: group by uckey + interval_starting_time + keyword
        df = df_logs_batched.groupBy('uckey', 'interval_starting_time', 'keyword_index').agg(
            first('keyword').alias('keyword'),
            fn.sum(col('is_click')).alias('keyword_click_count'),
            fn.count(fn.when(col('is_click') == 0, 1).otherwise(
                0)).alias('keyword_show_count')
        )
        df = df.withColumn('keyword_index_click_count', concat_ws(":", col('keyword_index'), col('keyword_click_count')))
        df = df.withColumn('keyword_index_show_count', concat_ws(":", col('keyword_index'), col('keyword_show_count')))
        df = df.withColumn('keyword_click_count', concat_ws(":", col('keyword'), col('keyword_click_count')))
        df = df.withColumn('keyword_show_count', concat_ws(":", col('keyword'), col('keyword_show_count')))

        # group 2: group by uckey + interval_starting_time
        df = df.groupBy('uckey', 'interval_starting_time').agg(
            concat_ws(",", collect_list('keyword_index')).alias('interval_keyword_indexes'),
            concat_ws(",", collect_list('keyword_index_click_count')).alias('interval_keyword_indexes_click_counts'),
            concat_ws(",", collect_list('keyword_index_show_count')).alias('interval_keyword_indexes_show_counts'),
            concat_ws(",", collect_list('keyword')).alias('interval_keywords'),
            concat_ws(",", collect_list('keyword_click_count')).alias('interval_keywords_click_counts'),
            concat_ws(",", collect_list('keyword_show_count')).alias('interval_keywords_show_counts')
        )
        return df

    def collect_trainready(df_trainready_batched_temp):
        # group 3: group by uckey with the temp batched uckey-interval_starting_time datasets.

        df = df_trainready_batched_temp

        # To improve performance, remove sorting and move it to each uckey.
        df = df.orderBy(
            [col('uckey'), col('interval_starting_time').desc()])

        df = df.groupBy('uckey').agg(
            collect_list('interval_starting_time').alias('interval_starting_time'),
            collect_list('interval_keyword_indexes').alias('keyword_indexes'),
            collect_list('interval_keyword_indexes_click_counts').alias('keyword_indexes_click_counts'),
            collect_list('interval_keyword_indexes_show_counts').alias('keyword_indexes_show_counts'),
            collect_list('interval_keywords').alias('keywords'),
            collect_list('interval_keywords_click_counts').alias('keywords_click_counts'),
            collect_list('interval_keywords_show_counts').alias('keywords_show_counts')
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

    trainready_table_temp = trainready_table_name + '_temp'
    timer_start = timeit.default_timer()
    batched_round = 1

    starting_time_sec = int(datetime.strptime(starting_date, "%Y-%m-%d").strftime("%s"))
    ending_time_sec = int(datetime.strptime(ending_date, "%Y-%m-%d").strftime("%s"))

    while starting_time_sec < ending_time_sec:
        batched_time_end_sec = starting_time_sec + timedelta(minutes=load_logs_in_minutes).total_seconds()

        command = """select distinct interval_starting_time from {} where action_time_seconds between {} and {}"""
        interval_starting_time_list = hive_context.sql(command.format(logs_table_name, starting_time_sec, batched_time_end_sec)).collect()

        interval_starting_time_list = [_['interval_starting_time'] for _ in interval_starting_time_list]
        interval_starting_time_list.sort()
        command = """select * from {} where interval_starting_time between {} and {}"""
        df_logs_batched = hive_context.sql(command.format(logs_table_name, interval_starting_time_list[0], interval_starting_time_list[-1]))

        print(str(batched_round) + ' batch for time_interval ' +
              str(interval_starting_time_list[0]) + ' - ' + str(interval_starting_time_list[-1]))

        df_trainready = group_batched_logs(df_logs_batched)
        mode = 'overwrite' if batched_round == 1 else 'append'
        write_to_table(df_trainready, trainready_table_temp, mode=mode)
        batched_round += 1

        # calculate new batched_time_end
        starting_time_sec = df_logs_batched.filter('interval_starting_time == {}'.format(
            interval_starting_time_list[-1])).agg(fn.max('action_time_seconds')).take(1)[0][0]
        starting_time_sec += 1

    # load the batched trainready data and merge them with the same uckey.
    df_trainready_batched_temp = load_df(hive_context, trainready_table_temp)
    df_trainready = collect_trainready(df_trainready_batched_temp)
    df_trainready = index_df_trainready(df_trainready)
    write_to_table(df_trainready, trainready_table_name, mode='overwrite')
    timer_end = timeit.default_timer()
    print('Total running time for processing batched trainready logs in seconds: ' + str(timer_end - timer_start))


def run(hive_context, cfg):
    starting_date = cfg['pipeline']['main_clean']['conditions']['starting_date']
    ending_date = cfg['pipeline']['main_clean']['conditions']['ending_date']
    load_logs_in_minutes = cfg['pipeline']['main_clean']['data_input']['load_logs_in_minutes']
    logs_table_name = cfg['pipeline']['main_logs']['logs_output_table_name']
    interval_time_in_seconds = cfg['pipeline']['main_logs']['interval_time_in_seconds']
    trainready_table_name = cfg['pipeline']['main_trainready']['trainready_output_table_name']

    load_and_group_batched_logs(hive_context, starting_date, ending_date, load_logs_in_minutes,
                                interval_time_in_seconds, logs_table_name, trainready_table_name)

if __name__ == "__main__":

    """
    This program performs the followings:
    adds normalized data by adding index of features
    groups data into time_intervals and ucdocs (labeled by uckey)
    """
    parser = argparse.ArgumentParser(description='processing data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log']['level'])

    run(hive_context=hive_context, cfg=cfg)

    sc.stop()
