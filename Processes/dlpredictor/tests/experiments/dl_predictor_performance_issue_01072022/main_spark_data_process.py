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


# -*- coding: UTF-8 -*-
import sys
import yaml
import argparse

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, create_map, sum as sum_agg, struct, explode
from pyspark.sql.types import IntegerType, StringType, ArrayType, MapType, FloatType, BooleanType
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
import pickle

from dlpredictor import transform
from dlpredictor.configutil import *
from dlpredictor.log import *
from dlpredictor.prediction.forecaster import Forecaster
from dlpredictor.util.sparkesutil import *


'''
spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 32G --driver-memory 32G --py-files dist/dlpredictor-1.6.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg,lib/predictor_dl_model-1.6.0-py2.7.egg --jars lib/elasticsearch-hadoop-6.8.0.jar experiments/dl_predictor_performance_issue/main_spark_data_process.py experiments/dl_predictor_performance_issue/config.yml '2021-07-21'
'''

# Reza
def add_count_arrays(ca1, ca2):
    '''
    ca1 = [u'1:9']
    '''
    result_map = {}
    for i in ca1+ca2:
        key, value = i.split(':')
        if key not in result_map:
            result_map[key] = 0
        result_map[key] += int(value)
    result = []
    for key, value in result_map.items():
        result.append(key+":"+str(value))
    return result

# Reza
def sum_day_count_array(day_count_arrays):
    '''
    [{u'2019-11-02': [u'1:9']}]
    '''
    result_map = {}
    for day_count_array in day_count_arrays:
        for item in day_count_array:
            for day, v in item.items():
                if not day:
                    continue
                if day not in result_map:
                    result_map[day] = []
                result_map[day] = add_count_arrays(result_map[day], v)

    return [result_map]


def sum_count_array(hour_counts):
    result_map = {}
    for item in hour_counts:
        for _, v in item.items():
            for i in v:
                key, value = i.split(':')
                if key not in result_map:
                    result_map[key] = 0
                result_map[key] += int(value)
    result = []
    for key, value in result_map.items():
        result.append(key + ":" + str(value))
    return result


def run(cfg, yesterday):
    sc = SparkContext()
    hive_context = HiveContext(sc)
    forecaster = Forecaster(cfg)
    sc.setLogLevel(cfg['log_level'])

    # Reading the max bucket_id
    dl_data_path = cfg['dl_predict_ready_path']
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata_area_map = cfg['area_map_table']
    distribution_table = cfg['distribution_table']
    norm_table = cfg['norm_table']
    dl_uckey_cluster_path = cfg['dl_uckey_cluster_path']

    # Reza
    # model_stats = get_model_stats_using_pickel(cfg)
    model_stat_table = cfg['model_stat_table']
    model_stats = get_model_stats(hive_context, model_stat_table)
    if not model_stats:
        sys.exit("dl_spark_cmd: " + "null model stats")

    # Read dist
    command = "SELECT DIST.uckey, DIST.ratio, DIST.cluster_uckey, DIST.price_cat FROM {} AS DIST ".format(
        distribution_table)

    df_dist = hive_context.sql(command)
    df_dist = df_dist.repartition("uckey")
    df_dist.cache()

    # create day_list from yesterday for train_window
    duration = model_stats['model']['duration']
    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, duration):
        day_list.append(datetime.strftime(day, '%Y-%m-%d'))
        day = day + timedelta(days=-1)
    day_list.sort()

    df_prediction_ready = None
    df_uckey_cluster = None
    start_bucket = 0
    global i
    i = sc.accumulator(0)

    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = " SELECT FACTDATA.count_array, FACTDATA.day, FACTDATA.hour, FACTDATA.uckey FROM {} AS FACTDATA WHERE FACTDATA.bucket_id BETWEEN {} AND {}  and FACTDATA.day in {}".format(
            factdata_area_map, str(start_bucket), str(end_bucket), tuple(day_list))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)
        # decrease partitions
        df = df.coalesce(200)

        if len(eligble_slot_ids) > 0:
            df = df.filter(udf(lambda x: eligble_slot_ids.__contains__(x.split(",")[1]), BooleanType())(df.uckey))
        df = df.withColumn('hour_price_imp_map',
                           expr("map(hour, count_array)"))

        df = df.groupBy('uckey', 'day').agg(
            collect_list('hour_price_imp_map').alias('hour_price_imp_map_list'))

        df = df.withColumn('day_price_imp', udf(
            sum_count_array, ArrayType(StringType()))(df.hour_price_imp_map_list)).drop('hour_price_imp_map_list')

        df = df.withColumn('day_price_imp_map', expr(
            "map(day, day_price_imp)"))

        df = df.groupBy('uckey').agg(collect_list(
            'day_price_imp_map').alias('day_price_imp_map_list'))

        df = df.join(df_dist, on=['uckey'], how='inner')
        df.cache()

        # df_uckey_cluster keeps the ratio and cluster_key for only uckeys that are being processed

        df_uckey_cluster = df.select('uckey', 'cluster_uckey', 'ratio', 'price_cat')

        df = df.groupBy('cluster_uckey', 'price_cat').agg(collect_list('day_price_imp_map_list').alias('cluster_day_price_imp_list'))
        df = df.withColumn('ts', udf(sum_day_count_array,
                                     ArrayType(MapType(StringType(), ArrayType(StringType()))))(
            df.cluster_day_price_imp_list))

        df = df.drop('cluster_day_price_imp_list')
        #dl_data_path = 'dl_prediction_ready'

        if i.value == 0:
            df.coalesce(100).write.mode('overwrite').parquet(dl_data_path)
            df_uckey_cluster.coalesce(100).write.mode('overwrite').parquet(dl_uckey_cluster_path)

        else:
            df.coalesce(100).write.mode('append').parquet(dl_data_path)
            df_uckey_cluster.coalesce(100).write.mode('append').parquet(dl_uckey_cluster_path)

        i += 1
        df.unpersist()

    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Post data process')
    parser.add_argument('config_file')
    parser.add_argument('yesterday', help='end date in yyyy-mm-dd formate')
    args = parser.parse_args()
    # Load config file
    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
            resolve_placeholder(cfg)

    except IOError as e:
        print(
            "Open config file unexpected error: I/O error({0}): {1}".format(e.errno, e.strerror))
    except Exception as e:
        print("Unexpected error:{}".format(sys.exc_info()[0]))
        raise
    finally:
        ymlfile.close()

    yesterday = args.yesterday

    eligble_slot_ids = cfg['eligble_slot_ids']
    yesterday = str(yesterday)

    run(cfg, yesterday)
