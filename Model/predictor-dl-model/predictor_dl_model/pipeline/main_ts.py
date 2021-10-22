# Copyright 2019, Futurewei Technologies
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

import math
import pickle
import statistics
import yaml
import argparse

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from util import resolve_placeholder

import transform as transform


def __save_as_table(df, table_name, hive_context, create_table):

    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

        command = """
            CREATE TABLE IF NOT EXISTS {}
            (
            uckey string,
            price_cat string,
            ts array<int>,
            a string,
            g string,
            t string,
            si string,
            r string,
            ipl string
            )
            """.format(table_name)

        hive_context.sql(command)

    df.select('uckey',
              'price_cat',
              'ts',
              'a',
              'g',
              't',
              'si',
              'r',
              'ipl'
              ).write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)


def normalize(mlist):
    avg = statistics.mean(mlist)
    std = statistics.stdev(mlist)
    return [0 if std == 0 else (item-avg)/(std) for item in mlist], avg, std


def run(hive_context, conditions, factdata_table_name, yesterday, past_days, output_table_name, bucket_size, bucket_step):

    # ts will be counts from yesterday-(past_days) to yesterday

    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, past_days):
        day_list.append(datetime.strftime(day, '%Y-%m-%d'))
        day = day + timedelta(days=-1)
    day_list.sort()

    start_bucket = 0
    first_round = True
    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = """
        SELECT count_array, day, hour, uckey FROM {} WHERE bucket_id BETWEEN {} AND {}
        """.format(factdata_table_name, str(start_bucket), str(end_bucket))

        if len(conditions) > 0:
            command = command + " AND {}".format(' AND '.join(conditions))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)
        # [Row(count_array=[u'0:0', u'1:0', u'2:0', u'3:0'], day=u'2018-03-09', hour=0, uckey=u'banner,1,3G,g_f,1,pt,1002,icc')]

        df = transform.add_count_map(df)
        # [Row(count_array=['3:4'], day='2019-11-02', hour=19, uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', count_map={'3': '4'})]

        # Explode count_map to have pcat and count on separate columns
        df = df.select('uckey', 'day', 'hour', explode(df.count_map)).withColumnRenamed(
            "key", "price_cat").withColumnRenamed("value", "count")
        # [Row(uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', day='2019-11-02', hour=19, price_cat='3', count='4')]

        # This is to have a model based on daily count, because the hourly data is very sparse
        df = df.groupBy('uckey', 'day', 'price_cat').agg(
            sum('count').alias('count'))
        # [Row(uckey='splash,5cd1c663263511e6af7500163e291137,WIFI,g_m,4,CPT,3,', day='2019-11-02', price_cat='1', count=56.0)]

        df = df.withColumn('day_count', expr("map(day, count)"))
        # [Row(uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', day='2019-11-02', price_cat='3', count='4', day_count={'2019-11-02': '4'})]

        df = df.groupBy('uckey', 'price_cat').agg(
            collect_list('day_count').alias('ts_list_map'))
        # [Row(uckey='native,l03493p0r3,4G,g_m,4,CPM,23,78', price_cat='1', ts_list_map=[{'2019-11-02': '13'}])]

        # This method handles missing dates by injecting nan
        df = transform.calculate_time_series(df, day_list)
        # [Row(uckey='native,l03493p0r3,4G,g_m,4,CPM,23,78', price_cat='1', ts_list_map=[{'2019-11-02': '13'}], ts=[nan, nan, nan, nan, nan, nan, nan, nan, nan, 2.6390573978424072])]

        # Log processor code to know the index of features
        # v = concat_ws(UCDoc.uckey_delimiter, df.adv_type 0 , df.slot_id 1 , df.net_type 2 , df.gender 3 , df.age 4 ,
        #                   df.price_dev 5 , df.pricing_type 6 , df.residence_city 7 , df.ip_city_code 8 )
        df = df.withColumn('a', transform.add_feature_udf(4)(df.uckey))
        df = df.withColumn('si', transform.add_feature_udf(1)(df.uckey))
        df = df.withColumn('r', transform.add_feature_udf(7)(df.uckey))
        df = df.withColumn('ipl', transform.add_feature_udf(8)(df.uckey))
        df = df.withColumn('t', transform.add_feature_udf(2)(df.uckey))
        df = df.withColumn('g', transform.add_feature_udf(3)(df.uckey))

        __save_as_table(df, output_table_name, hive_context, first_round)
        first_round = False


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    cfg_log = cfg['log']
    cfg = cfg['pipeline']['time_series']

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg_log['level'])

    yesterday = cfg['yesterday']
    prepare_past_days = cfg['prepare_past_days']
    output_table_name = cfg['output_table_name']
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    input_table_name = cfg['input_table_name']
    conditions = cfg['conditions']

    run(hive_context, conditions, input_table_name,
        yesterday, prepare_past_days, output_table_name, bucket_size, bucket_step)

    sc.stop()
