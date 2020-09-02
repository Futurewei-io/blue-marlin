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
import timeit
import numpy as np
import pyspark.sql.functions as fn
import pyspark.sql.types as t

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import concat_ws, col
from datetime import datetime, timedelta
from util import load_df, write_to_table, drop_table
from pyspark.sql.window import Window


def fit_distribution(df):
    raw_impressions = [547788492, 113155690, 357229507, 353837519, 231243807, 343536283, 204197454, 298400019,
                       154702872,
                       101595992, 173078606, 245617500, 210661722, 94400758, 100694511, 104621562, 47302450, 254680986,
                       38653660, 118198547, 167100705, 484483594, 69681730, 230778513, 109614764, 86169134, 125825905,
                       478045002, 476281884, 155086936, 100034338, 140623283, 132801777, 150906980, 108772611, 2165682,
                       41589670, 327990489, 85909446, 349940682, 8776893, 33502930, 282401283, 82297276, 385473488,
                       219411532, 154739307, 51048940, 192605283, 114587775, 230422182, 41743162, 103709711, 171397519,
                       158854098, 105911195, 118981954, 78965914, 91906274, 158792685, 63487656, 54706539, 111072455,
                       92442258, 150615474, 79697857, 108585993, 112360549, 262424361, 494715712, 1152693549,
                       1035303850,
                       324325907, 921851042, 390727201, 1257338071, 392629713, 778974819, 129782245, 1683290505,
                       811910155,
                       1997598872]
    total_impressions = sum(raw_impressions)
    region_percentage = {i + 1: float(x) / float(total_impressions)
                         for i, x in enumerate(raw_impressions)}
    cum_percentage = list(np.cumsum(region_percentage.values()))
    cum_pairs = zip([0]+cum_percentage[:-1], cum_percentage)
    cum_map = {x: i + 1 for i, x in enumerate(cum_pairs)}
    df1 = df.withColumn('uid', fn.monotonically_increasing_id())
    w = Window.partitionBy('uckey').orderBy('uid')
    df2 = df1.withColumn('cum_id', fn.cume_dist().over(w))

    def lookup_ipl(value):
        for pair in cum_pairs:
            low, high = pair
            if low <= value and value < high or value == 1.0:
                return cum_map[pair]
        return -1
    _udf_1 = fn.udf(lookup_ipl, t.IntegerType())
    df3 = df2.withColumn('index', _udf_1(fn.col('cum_id')))
    df3 = df3.drop(df3.uid).drop(df3.cum_id)
    return df3


def load_and_regionalize_batched_logs(hive_context, starting_date, ending_date, load_logs_in_minutes, logs_table_name):
    timer_start = timeit.default_timer()
    batched_round = 1
    starting_time = datetime.strptime(starting_date, "%Y-%m-%d")
    ending_time = datetime.strptime(ending_date, "%Y-%m-%d")
    logs_table_temp_name = logs_table_name+'_temp'
    while starting_time < ending_time:
        # data clean for showlog table.
        batched_time_start_str = starting_time.strftime("%Y-%m-%d %H:%M:%S")
        batched_time_end = starting_time + timedelta(minutes=load_logs_in_minutes)
        batched_time_end_str = batched_time_end.strftime("%Y-%m-%d %H:%M:%S")
        print('Batch ' + str(batched_round) + ': processing batched logs in time range ' +
              batched_time_start_str + ' - ' + batched_time_end_str)
        command = """select * from {} where action_time >= '{}' and action_time < '{}'"""
        df_logs_batched = hive_context.sql(command.format(logs_table_name, batched_time_start_str, batched_time_end_str))
        df_logs_batched = df_logs_batched.drop(col('region_id'))
        df_logs_batched = fit_distribution(df_logs_batched)
        df_logs_batched = df_logs_batched.withColumnRenamed('index', 'region_id')
        df_logs_batched = df_logs_batched.withColumn('uckey', concat_ws(",", col('media'), col('media_category'), col('net_type'), col('gender'), col('age'), col('region_id')))

        mode = 'overwrite' if batched_round == 1 else 'append'
        write_to_table(df_logs_batched, logs_table_temp_name, mode=mode)
        batched_round += 1
        starting_time = batched_time_end

    # use the temp table to save all the batched logs with region id inside it.
    # drop the logs table and alter the temp table name to the logs table.
    drop_table(hive_context, logs_table_name)
    command = """alter table {} rename to {}""".format(logs_table_temp_name, logs_table_name)
    hive_context.sql(command)

    timer_end = timeit.default_timer()
    print('Total running time for processing batched logs in seconds: ' +
          str(timer_end - timer_start))


def run(hive_context, cfg):
    starting_date = cfg['pipeline']['main_clean']['conditions']['starting_date']
    ending_date = cfg['pipeline']['main_clean']['conditions']['ending_date']
    load_logs_in_minutes = cfg['pipeline']['main_clean']['data_input']['load_logs_in_minutes']
    logs_table_name = cfg['pipeline']['main_logs']['logs_output_table_name']

    load_and_regionalize_batched_logs(hive_context, starting_date, ending_date, load_logs_in_minutes, logs_table_name)


if __name__ == "__main__":

    """
    This is an optional step only for the logs data without any region information.
    If the original logs have the geo location or region information(ipl or  r), ignore this step.    
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
