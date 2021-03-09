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
from pyspark.sql.functions import lit, col, udf, collect_list, concat_ws, first, create_map, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, ArrayType, StringType, LongType
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from util import write_to_table, write_to_table_with_partition, print_batching_info, resolve_placeholder, load_config, load_batch_config, load_df
from itertools import chain

MAX_USER_IN_BUCKET = 10**9


def date_to_timestamp(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds())


def generate_trainready(hive_context, batch_config,
                        interval_time_in_seconds,
                        logs_table_name, trainready_table, did_bucket_num):

    def group_batched_logs(df_logs):
        # group logs from did + interval_time + keyword.
        # group 1: group by did + interval_starting_time + keyword
        df = df_logs.groupBy('did', 'interval_starting_time', 'keyword_index').agg(
            first('keyword').alias('keyword'),
            first('age').alias('age'),
            first('gender').alias('gender'),
            first('did_bucket').alias('did_bucket'),
            fn.sum(col('is_click')).alias('kw_clicks_count'),
            fn.sum(fn.when(col('is_click') == 0, 1).otherwise(0)).alias('kw_shows_count'),
        )

        df = df.withColumn('kwi_clicks_count', concat_ws(":", col('keyword_index'), col('kw_clicks_count')))
        df = df.withColumn('kwi_shows_count', concat_ws(":", col('keyword_index'), col('kw_shows_count')))
        df = df.withColumn('kw_clicks_count', concat_ws(":", col('keyword'), col('kw_clicks_count')))
        df = df.withColumn('kw_shows_count', concat_ws(":", col('keyword'), col('kw_shows_count')))

        # group 2: group by did + interval_starting_time
        df = df.groupBy('did', 'interval_starting_time').agg(
            concat_ws(",", collect_list('keyword_index')).alias('kwi'),
            concat_ws(",", collect_list('kwi_clicks_count')).alias('kwi_click_counts'),
            concat_ws(",", collect_list('kwi_shows_count')).alias('kwi_show_counts'),
            concat_ws(",", collect_list('keyword')).alias('interval_keywords'),
            concat_ws(",", collect_list('kw_clicks_count')).alias('kw_click_counts'),
            concat_ws(",", collect_list('kw_shows_count')).alias('kw_show_counts'),
            first('age').alias('age'),
            first('gender').alias('gender'),
            first('did_bucket').alias('did_bucket')
        )

        return df

    def collect_trainready(df_trainready_batched_temp):
        # group 3: group by did with the temp batched did-interval rows.

        df = df_trainready_batched_temp

        features = ['interval_starting_time', 'interval_keywords', 'kwi', 'kwi_click_counts', 'kwi_show_counts']
        agg_attr_list = list(chain(*[(lit(attr), col(attr)) for attr in df.columns if attr in features]))
        df = df.withColumn('attr_map', create_map(agg_attr_list))

        df = df.groupBy('did').agg(
            collect_list('attr_map').alias('attr_map_list'),
            first('age').alias('age'),
            first('gender').alias('gender'),
            first('did_bucket').alias('did_bucket')
        )

        return df

    def build_feature_array(df):
        '''
        df['attr_map_list']=
        [{u'kwi': u'14', u'interval_starting_time': u'1576713600', u'kwi_show_counts': u'14:2', u'kwi_click_counts': u'14:0', u'interval_keywords': u'info'}, 
        {u'kwi': u'14,29', u'interval_starting_time': u'1576886400', u'kwi_show_counts': u'14:2,29:4', u'kwi_click_counts': u'14:0,29:0', u'interval_keywords': u'info,video'}, 
        {u'kwi': u'14', u'interval_starting_time': u'1576800000', u'kwi_show_counts': u'14:4', u'kwi_click_counts': u'14:0', u'interval_keywords': u'info'}], 
        '''
        def udf_function(attr_map_list):
            tmp_list = []
            for _dict in attr_map_list:
                tmp_list.append((_dict['interval_starting_time'], _dict))
            tmp_list.sort(reverse=True, key=lambda x: x[0])

            interval_starting_time = []
            interval_keywords = []
            kwi = []
            kwi_show_counts = []
            kwi_click_counts = []
            for time, _dict in tmp_list:
                interval_starting_time.append(str(time))
                interval_keywords.append(_dict['interval_keywords'])
                kwi.append(_dict['kwi'])
                kwi_show_counts.append(_dict['kwi_show_counts'])
                kwi_click_counts.append(_dict['kwi_click_counts'])
            return [interval_starting_time, interval_keywords, kwi, kwi_show_counts, kwi_click_counts]
        df = df.withColumn('metrics_list', udf(udf_function, ArrayType(ArrayType(StringType())))(col('attr_map_list')))
        return df

    trainready_table_temp = trainready_table + '_temp'
    timer_start = timeit.default_timer()

    '''
        1. Find the intervals per user did.
        2. Agg on time and kewords so that we have one record be user for each interval.
        e.g.
        interval = day
        unique users per day = 100m
        number of records per interval = 100m
    '''

    start_date, end_date, load_minutes = batch_config
    starting_time = datetime.strptime(start_date, "%Y-%m-%d")
    ending_time = datetime.strptime(end_date, "%Y-%m-%d")

    all_intervals = set()
    st = date_to_timestamp(starting_time)
    et = date_to_timestamp(ending_time)
    x = st
    while x < et:
        interval_point = x - x % interval_time_in_seconds
        all_intervals.add(interval_point)
        x += interval_time_in_seconds
    all_intervals = list(all_intervals)
    all_intervals.sort()

    batched_round = 1
    for did_bucket in range(did_bucket_num):
        for interval_point in all_intervals:
            '''
            We need the days since we have days partitions.
            '''
            day_lower = datetime.fromtimestamp(interval_point).strftime("%Y-%m-%d")
            day_upper = datetime.fromtimestamp(interval_point+interval_time_in_seconds).strftime("%Y-%m-%d")

            command = """SELECT * 
                        FROM {} 
                        WHERE 
                        day >= '{}' AND day <= '{}' AND  
                        interval_starting_time = '{}' AND 
                        did_bucket= '{}' """
            df_logs = hive_context.sql(command.format(logs_table_name, day_lower, day_upper, interval_point, did_bucket))

            df_trainready = group_batched_logs(df_logs)

            mode = 'overwrite' if batched_round == 1 else 'append'
            write_to_table_with_partition(df_trainready, trainready_table_temp, partition=('did_bucket'), mode=mode)
            batched_round += 1

    '''
    Now we need to agg for one user over all days to create the whole record.
    e.g.
    For 
        100 days
        100M unique users per day
        10 User buckets
    We need cluster that can fit 1000M=1G records.
    If not possible we need to increase user bucket number.
    '''
    trainready_table_temp
    batched_round = 1
    for did_bucket in range(did_bucket_num):
        command = """SELECT * 
                        FROM {} 
                        WHERE 
                        did_bucket= '{}' """
        df = hive_context.sql(command.format(trainready_table_temp, did_bucket))
        df = collect_trainready(df)
        df = build_feature_array(df)
        '''
        at this point df is like below
        [Row(age=6, gender=0, did=u'773e03d2bc89d49c0c9c60270ee650e555abdf32cf5305c9fe27f081e1e64d91', metrics_list=[[u'1576800000'], [u'25'], [u'25:1'], [u'25:0']], did_bucket=u'0')]
        '''
        for i, feature_name in enumerate(['interval_starting_time', 'interval_keywords', 'kwi', 'kwi_show_counts', 'kwi_click_counts']):
            df = df.withColumn(feature_name, col('metrics_list').getItem(i))

        # Add did_index
        w = Window.orderBy("did_bucket", "did")
        df = df.withColumn('row_number', row_number().over(w))
        df = df.withColumn('did_index', udf(lambda x: did_bucket*(MAX_USER_IN_BUCKET) + x, LongType())(col('row_number')))
        df = df.select('age', 'gender', 'did', 'did_index', 'interval_starting_time', 'interval_keywords',
                       'kwi', 'kwi_show_counts', 'kwi_click_counts', 'did_bucket')

        mode = 'overwrite' if batched_round == 1 else 'append'
        write_to_table_with_partition(df, trainready_table, partition=('did_bucket'), mode=mode)
        batched_round += 1

    return


def run(hive_context, cfg):
    cfg_logs = cfg['pipeline']['main_logs']
    cfg_clean = cfg['pipeline']['main_clean']
    logs_table_name = cfg_logs['logs_output_table_name']
    interval_time_in_seconds = cfg_logs['interval_time_in_seconds']

    cfg_train = cfg['pipeline']['main_trainready']
    trainready_table = cfg_train['trainready_output_table']
    did_bucket_num = cfg_clean['did_bucket_num']

    batch_config = load_batch_config(cfg)

    generate_trainready(hive_context, batch_config, interval_time_in_seconds, logs_table_name, trainready_table, did_bucket_num)


if __name__ == "__main__":
    """
    This program performs the followings:
    adds normalized data by adding index of features
    groups data into time_intervals and dids (labeled by did)
    """
    sc, hive_context, cfg = load_config(description="pre-processing train ready data")
    resolve_placeholder(cfg)
    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
