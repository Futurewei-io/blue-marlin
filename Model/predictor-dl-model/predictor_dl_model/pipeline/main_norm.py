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

import math
import pickle
import statistics
import yaml
import argparse
import json

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, avg, stddev, rand
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, FloatType, BooleanType, StructType, \
    StructField
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

        df.createOrReplaceTempView("r900_temp_table")

        command = """
            CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM r900_temp_table
            """.format(table_name)

        hive_context.sql(command)


def add_ohe_feature(df, feature_name, feature_value_list):
    '''
    This method is generalization of
    df = df.withColumn('g_x', udf(lambda x: x['g_x'], FloatType())(df.g))
    feature_name : a feature_value_list : [1,2,3]
    '''

    def _helper(df, feature_name, feature_value):
        ohe_feature = feature_name + '_' + str(feature_value)
        df = df.withColumn(ohe_feature, udf(
            lambda x: float(x[feature_value]) if feature_value in x else 0.0, FloatType())(df[feature_name]))
        return df

    for feature_value in feature_value_list:
        df = _helper(df, feature_name, feature_value)

    return df


def normalize(mlist):
    avg = statistics.mean(mlist)
    std = statistics.stdev(mlist)
    return [0 if std == 0 else (item - avg) / (std) for item in mlist], avg, std


def run(sc, hive_context, columns, input_table_name, output_table_name, yesterday, prepare_past_days, holidays,
        model_info,
        model_table, new_si_list):
    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, prepare_past_days):
        day_list.append(datetime.strftime(day, '%Y-%m-%d'))
        day = day + timedelta(days=-1)
    day_list.sort()

    model_stats = {}
    tsf = {}

    holidays = [1 if day in holidays else 0 for day in day_list]
    holidays_norm, hol_avg, hol_std = normalize(holidays)

    model_info['days'] = list(map(str, day_list))
    model_info['holidays_norm'] = holidays_norm
    model_stats['holiday_stats'] = [hol_avg, hol_std]

    command = """
    SELECT uckey, ts, price_cat, a, g, t, si, p FROM {}
    """.format(input_table_name)

    # DataFrame[uckey: string, price_cat: string, ts: array<int>, a: string, g: string, t: string, si: string, r: string, ipl: string]
    df = hive_context.sql(command)

    si_list = []
    ipl_list = []
    removed_columns = []
    for feature_name, feature_value_list in columns.items():
        if feature_name == 'price_cat':
            df = transform.add_ohe_feature(
                df, feature_name, feature_value_list)
        else:
            df = add_ohe_feature(df, feature_name, feature_value_list)
        if feature_name != 'price_cat':
            removed_columns.append(feature_name)
        for feature_value in feature_value_list:
            ohe_feature = feature_name + '_' + str(feature_value)
            ohe_feature_n = ohe_feature + '_n'
            if feature_name == 'si':
                si_list.append(ohe_feature_n)
            if feature_name == 'ipl':
                ipl_list.append(ohe_feature_n)
            df, stats = transform.normalize_ohe_feature(
                df, ohe_feature=ohe_feature)
            removed_columns.append(ohe_feature)
            model_stats[ohe_feature] = stats

    df = df.drop(*removed_columns)

    # Create vector from sis nd r
    if len(si_list) > 0:
        df = df.withColumn('si_vec_n', udf(
            lambda *x: [_ for _ in x], ArrayType(FloatType()))(*si_list))
        df = df.drop(*si_list)

    if len(ipl_list):
        df = df.withColumn('ipl_vec_n', udf(
            lambda *x: [_ for _ in x], ArrayType(FloatType()))(*ipl_list))
        df = df.drop(*ipl_list)

    df, stats = transform.normalize_ohe_feature(df, ohe_feature='p')
    model_stats['page_popularity'] = stats

    tsf['stats'] = model_stats
    tsf['model_info'] = model_info
    tsf['si_list'] = new_si_list

    df = df.withColumn('ts_n', udf(lambda ts: [math.log(
        count + 1) for count in ts], ArrayType(FloatType()))(df.ts))

    df = df.withColumn('page_ix', udf(lambda x, y: x + '-' + y,
                                      StringType())(df.uckey, df.price_cat))

    __save_as_table(df, output_table_name, hive_context, True)

    # save tsf as hive table
    json_object = json.dumps(tsf)
    rdd = sc.parallelize((json_object,))
    df_model = hive_context.read.json(rdd)

    __save_as_table(df_model, model_table, hive_context, True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    cfg_log = cfg['log']
    cfg_pipeline = cfg['pipeline']

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg_log['level'])

    input_table_name = cfg_pipeline['uckey_clustering']['output_table_name']
    columns = cfg_pipeline['normalization']['columns']
    output_table_name = cfg_pipeline['normalization']['output_table_name']

    yesterday = cfg_pipeline['time_series']['yesterday']
    prepare_past_days = cfg_pipeline['time_series']['prepare_past_days']
    holidays = cfg_pipeline['normalization']['holidays']
    tf_statistics_path = cfg_pipeline['tfrecords']['tf_statistics_path']
    new_si_list = cfg_pipeline['filter']['new_si_list']
    model_table = cfg['save_model']['table']

    model_info = {
        "name": cfg['save_model']['model_name'],
        "version": cfg['save_model']['model_version'],
        "duration": prepare_past_days,
        "train_window": cfg['save_model']['train_window'],
        "predict_window": cfg['trainer']['predict_window']
    }

    run(sc=sc, hive_context=hive_context, columns=columns,
        input_table_name=input_table_name, output_table_name=output_table_name,
        yesterday=yesterday, prepare_past_days=prepare_past_days, holidays=holidays,
        model_info=model_info, model_table=model_table, new_si_list=new_si_list)

    sc.stop()
