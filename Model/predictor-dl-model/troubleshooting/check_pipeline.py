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

import unittest
import math
import pickle
import statistics
import yaml
import argparse
import re
import hashlib

import pyspark.sql.functions as fn
import numpy as np

from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import IntegerType, StringType, MapType
from datetime import datetime, timedelta

'''
How to run?

spark-submit --master yarn --num-executors 5 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict troubleshooting/check_pipeline.py
 test_pipeline.py >> check_pipeline_output.txt

Note, the file uses config.yml
'''


def resolve_placeholder(in_dict):
    stack = []
    for key in in_dict.keys():
        stack.append((in_dict, key))
    while len(stack) > 0:
        (_dict, key) = stack.pop()
        value = _dict[key]
        if type(value) == dict:
            for _key in value.keys():
                stack.append((value, _key))
        elif type(value) == str:
            z = re.findall('\{(.*?)\}', value)
            if len(z) > 0:
                new_value = value
                for item in z:
                    if item in in_dict and type(in_dict[item]) == str:
                        new_value = new_value.replace('{'+item+'}', in_dict[item])
                _dict[key] = new_value


def get_bucket_id(bucket_size, uckey):
    def __hash_sha256(s):
        hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
        return int(hex_value, 16)
    return __hash_sha256(uckey) % bucket_size


def get_training_days(yesterday, past_days):
    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, past_days):
        day_list.append(datetime.strftime(day, '%Y-%m-%d'))
        day = day + timedelta(days=-1)
    day_list.sort()
    return day_list


def build_ts_from_factdata(hive_context, days, uckey, price_cat):
    bucket_id = get_bucket_id(g_factdata_bucket_size, uckey)
    command = """
        SELECT count_array, day, hour, uckey FROM {} WHERE bucket_id='{}' AND uckey='{}'
        """.format(factdata_table, str(bucket_id), uckey)
    df = hive_context.sql(command)
    l = df.collect()
    day_count = {}
    for record in l:
        count_array = record['count_array']
        day = record['day']
        if day not in day_count:
            day_count[day] = 0

        for item in count_array:
            key_value = item.split(':')
            if key_value[0] == str(price_cat):
                day_count[day] += int(key_value[1])

    result = []
    for day in days:
        if day in day_count:
            result.append(day_count[day])
        else:
            result.append(0)

    return result


def calculate_factdata_traffic(hive_context, factdata_table, bucket_id, day):

    def _list_to_map(count_array):
        count_map = {}
        for item in count_array:
            key_value = item.split(':')
            count_map[key_value[0]] = key_value[1]
        return count_map

    command = """
        SELECT
        FACTDATA.count_array,
        FACTDATA.day,
        FACTDATA.hour,
        FACTDATA.uckey
        FROM {} AS FACTDATA
        WHERE FACTDATA.bucket_id='{}' AND day='{}'
        """.format(factdata_table, str(bucket_id), str(day))

    df = hive_context.sql(command)
    list_to_map_udf = fn.udf(_list_to_map, MapType(StringType(), StringType(), False))
    df = df.withColumn('count_map', list_to_map_udf(df.count_array))
    df = df.select('uckey', 'day', 'hour', fn.explode(df.count_map)).withColumnRenamed("key", "price_cat").withColumnRenamed("value", "count")
    # [Row(uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', day='2019-11-02', hour=19, price_cat='3', count='4')]

    return df.groupby().agg(fn.sum('count').alias('count')).take(1)[0]['count']


class TestMainClean(unittest.TestCase):

    def setUp(self):
        # Set the log level.
        sc = SparkContext.getOrCreate()
        sc.setLogLevel('ERROR')

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.hive_context = HiveContext(sc)

        command = 'SELECT * FROM {}'.format(distribution_details_table)
        self.df_dist_details = self.hive_context.sql(command)
        self.df_dist_details.cache()

        command = 'SELECT * FROM {}'.format(trainready_table)
        self.df_trainready = self.hive_context.sql(command)
        self.df_trainready.cache()

        command = 'SELECT * FROM {}'.format(model_stat_table)
        df_model_stat = self.hive_context.sql(command)
        self.model_stat = df_model_stat.collect()[0]

        command = 'SELECT * FROM {} WHERE bucket_id=0'.format(factdata_table)
        self.df_factdata = self.hive_context.sql(command)

    def test_dense_over_total_traffic(self):
        df = self.df_dist_details
        dense_traffic = df.where('ratio=1').agg(fn.sum('imp')).take(1)[0]['sum(imp)']
        total_traffic = df.agg(fn.sum('imp')).take(1)[0]['sum(imp)']
        dense_over_total = dense_traffic * 100 / total_traffic
        print('dense/total traffic:{}'.format(dense_over_total))
        self.assertTrue(dense_over_total > DENSE_OVER_TOTAL_TRAFFIC_PERCENAGE_TH)

    def test_get_traffic_statistic(self):
        print('number of uckeys in trainready : {:,}'.format(self.df_trainready.count()))
        print('total traffic in distribution deatils : {:,}'.format(self.df_dist_details.agg(fn.sum('imp')).take(1)[0]['sum(imp)']))
        ts_days = self.model_stat['model_info']['days']
        print('todatl traffic in one factdata bucket : {:,}'.format(1000 * len(ts_days) * calculate_factdata_traffic(self.hive_context, factdata_table, 0, g_days[-1])))

    def test_get_model_info(self):
        print('days from config {}'.format(str(g_days)))
        print('model info {}'.format(str(self.model_stat)))

    def test_get_trainready_statistics(self):
        df = self.df_trainready

        v = df.select('p','ts').orderBy(fn.col('p').asc()).head(1)
        print('min trainready P {}:'.format(v))

        v = v = df.select('p','ts').orderBy(fn.col('p').desc()).head(1)
        print('max trainready P {}:'.format(v))

    def test_training_days_length(self):
        l = self.df_trainready.take(NUMBER_OF_CHECK_UCKEYS)
        for _ in l:
            ts_days = self.model_stat['model_info']['days']
            if len(_['ts']) != len(ts_days):
                print('len ts is {}'.format(str(len(_['ts']))))
                self.assertTrue(len(_['ts']) == len(ts_days))

    def __test_ts_against_log(self):
        df = self.df_trainready.join(self.df_dist_details, on=['uckey', 'price_cat'], how='inner')
        l = df.where('ratio=1').take(NUMBER_OF_CHECK_UCKEYS)
        uckeys = [(_['uckey'], _['price_cat'], _['ts']) for _ in l]

        ts_days = self.model_stat['model_info']['days']
        for uckey, price_cat, ts in uckeys:
            factdata_ts = build_ts_from_factdata(self.hive_context, ts_days, uckey, price_cat)
            diff = np.array(factdata_ts)-np.array(ts)
            print(diff, uckey, price_cat)
            self.assertTrue(abs(np.sum(diff)) < TS_DIFF_SUM_TH)


# Runs the tests.
if __name__ == '__main__':

    DENSE_OVER_TOTAL_TRAFFIC_PERCENAGE_TH = 90
    TS_DIFF_SUM_TH = 10
    NUMBER_OF_CHECK_UCKEYS = 1

    with open('predictor_dl_model/config.yml', 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    distribution_details_table = cfg['pipeline']['distribution']['output_detail_table_name']
    trainready_table = cfg['pipeline']['normalization']['output_table_name']
    factdata_table = cfg['factdata_table_name']
    model_stat_table = cfg['save_model']['table']
    g_factdata_bucket_size = cfg['pipeline']['filter']['bucket_size']
    g_past_days = cfg['pipeline']['time_series']['prepare_past_days']
    g_yesterday = cfg['pipeline']['time_series']['yesterday']
    g_days = get_training_days(g_yesterday, g_past_days)

    # Run the unit tests.
    unittest.main()
