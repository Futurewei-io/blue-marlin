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
import numpy as np
import pyspark.sql.functions as fn
import pyspark.sql.types as t

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import concat_ws, col
from datetime import datetime, timedelta
from din_model.pipeline.util import load_config, load_df, load_batch_config
from din_model.pipeline.util import write_to_table, drop_table, print_batching_info
from pyspark.sql.window import Window


def fit_distribution(df):
    raw_impressions = [
        547788492, 113155690, 357229507, 353837519, 231243807, 
        343536283, 204197454, 298400019, 154702872, 101595992, 
        173078606, 245617500, 210661722, 94400758, 100694511, 
        104621562, 47302450, 254680986, 38653660, 118198547, 
        167100705, 484483594, 69681730, 230778513, 109614764, 
        86169134, 125825905, 478045002, 476281884, 155086936, 
        100034338, 140623283, 132801777, 150906980, 108772611, 
        2165682, 41589670, 327990489, 85909446, 349940682, 
        8776893, 33502930, 282401283, 82297276, 385473488,
        219411532, 154739307, 51048940, 192605283, 114587775, 
        230422182, 41743162, 103709711, 171397519, 158854098, 
        105911195, 118981954, 78965914, 91906274, 158792685, 
        63487656, 54706539, 111072455, 92442258, 150615474, 
        79697857, 108585993, 112360549, 262424361, 494715712, 
        1152693549, 1035303850, 324325907, 921851042, 390727201, 
        1257338071, 392629713, 778974819, 129782245, 1683290505, 
        811910155, 1997598872]
    total_impressions = sum(raw_impressions)
    # calculate the region percentage and store it in a dict of
    # region ID --> percentage of number of impressions in that region ID
    region_percentage = {i + 1: float(x) / float(total_impressions)
                         for i, x in enumerate(raw_impressions)}
    # cumulatively sum up the region percentage's value
    # so that cum_percentage is
    # [region_percentage[0], region_percentage[0] + region_percentage[1], ...]
    cum_percentage = list(np.cumsum(region_percentage.values()))
    # forming a list of tuple with interval of [lower_end, upper_end)
    cum_pairs = zip([0]+cum_percentage[:-1], cum_percentage)
    # for reverse lookup, map of
    # [lower_end, upper_end) --> region ID
    cum_map = {x: i + 1 for i, x in enumerate(cum_pairs)}
    # add a new column with uid
    # pyspark.sql.functions.monotonically_increasing_id()
    # A column that generates monotonically increasing 64-bit integers.
    # The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive. The current
    # implementation puts the partition ID in the upper 31 bits, and the record number within each partition in the
    # lower 33 bits. The assumption is that the data frame has less than 1 billion partitions, and each partition has
    # less than 8 billion records.
    # As an example, consider a DataFrame with two partitions, each with 3 records. This expression would return the
    # following IDs: 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
    df1 = df.withColumn('uid', fn.monotonically_increasing_id())
    # create a window function to partition the df by uckey, and order within each partition by uid
    w = Window.partitionBy('uckey').orderBy('uid')
    # pyspark.sql.functions.cume_dist()
    # Window function: returns the cumulative distribution of values within a window partition, i.e. the fraction of
    # rows that are below the current row.
    # Because ordered by UID, which is globally unique, and unique within each partition, each row will always be below
    # its successive rows.
    # This becomes a cumulative percentage of number of rows. e.g. assuming we have dataset of 10 rows, cumu_dist will
    # be [0.1, 0.2, 0.3, ...] == [i / total number of rows in partition for i from 1 to number of rows]
    # In any partitions, the PDF of a single row is 1 / rows b/c UID is unique
    # Therefore, the CDF of the partition becomes [1 / rows, 2 / rows, ...]
    df2 = df1.withColumn('cum_id', fn.cume_dist().over(w))

    def lookup_ipl(value):
        # naive algorithm, O(n)
        # for each (lower_end, upper_end) pair in the map, find out if the given 'value' is in the range of
        # lower_end and upper_end. If yes, return that region ID
        for pair in cum_pairs:
            low, high = pair
            if low <= value < high or value == 1.0:
                return cum_map[pair]
        return -1
    _udf_1 = fn.udf(lookup_ipl, t.IntegerType())
    df3 = df2.withColumn('index', _udf_1(fn.col('cum_id')))
    df3 = df3.drop(df3.uid).drop(df3.cum_id)
    return df3


def add_region_to_logs(hive_context, batch_config, logs_table):
    start_date, end_date, load_minutes = batch_config
    timer_start = timeit.default_timer()
    batched = 1
    starting_time = datetime.strptime(start_date, "%Y-%m-%d")
    ending_time = datetime.strptime(end_date, "%Y-%m-%d")
    logs_table_temp_name = logs_table+'_temp'
    while starting_time < ending_time:
        # data clean for showlog table.
        time_start_str = starting_time.strftime("%Y-%m-%d %H:%M:%S")
        batched_time_end = starting_time + timedelta(minutes=load_minutes)
        time_end_str = batched_time_end.strftime("%Y-%m-%d %H:%M:%S")
        print_batching_info("Main regions", batched, time_start_str, time_end_str)
        command = """select * from {} where action_time >= '{}' and action_time < '{}'"""
        logs = hive_context.sql(command.format(logs_table, time_start_str, time_end_str))
        logs = logs.drop(col('region_id'))
        logs = fit_distribution(logs)
        logs = logs.withColumnRenamed('index', 'region_id')
        logs = logs.withColumn('uckey', concat_ws(",", col('media'), 
                               col('media_category'), col('net_type'), 
                               col('gender'), col('age'), col('region_id')))

        mode = 'overwrite' if batched == 1 else 'append'
        write_to_table(logs, logs_table_temp_name, mode=mode)
        batched += 1
        starting_time = batched_time_end

    # use the temp table to save all the batched logs with region id inside it.
    # drop the logs table and alter the temp table name to the logs table.
    drop_table(hive_context, logs_table)
    command = """alter table {} rename to {}""".format(
                      logs_table_temp_name, logs_table)
    hive_context.sql(command)

    timer_end = timeit.default_timer()
    print('Total batching seconds: ' + str(timer_end - timer_start))


def run(hive_context, cfg):
    batch_config = load_batch_config(cfg)
    cfg_logs = cfg['pipeline']['main_logs']
    logs_table = cfg_logs['logs_output_table_name']
    # add region ids to logs.
    add_region_to_logs(hive_context, batch_config, logs_table)


if __name__ == "__main__":

    """
    This is an optional step only for the logs data without regions.
    If original logs have the geo info or region(ipl or  r), ignore this.    
    """
    sc, hive_context, cfg = load_config(description="main logs with regions")
    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
