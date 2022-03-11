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

This file operates like check_model but only produces the output, no verification.

This script performs the following actions:

1. call model API with N number of randomly picked dense uckeys from trainready (The same data that is used to train the model).
2. calculate the accuracy of the model.


run by:
spark-submit --master yarn --num-executors 5 --executor-cores 3 --executor-memory 16G --driver-memory 16G get_model_output.py

'''

from client_rest_dl2 import predict


def c_error(x, y):
    x = x * 1.0
    if x != 0:
        e = abs(x - y) / x
    else:
        e = -1
    e = round(e, 3)
    return e


def error_m(a, p):
    result = []
    for i in range(len(a)):
        x = a[i]
        y = p[i]
        e = c_error(x, y)
        result.append(e)
    x = sum(a)
    y = sum(p)
    e = c_error(x, y)
    return (e, result)


def normalize_ts(ts):
    ts_n = [math.log(i + 1) for i in ts]
    return ts_n


def dl_daily_forecast(serving_url, model_stats, day_list, ucdoc_attribute_map):
    x, y = predict(serving_url=serving_url, model_stats=model_stats,
                   day_list=day_list, ucdoc_attribute_map=ucdoc_attribute_map, forward_offset=0)
    ts = x[0]
    days = y
    return ts, days


def get_model_stats(hive_context, model_stat_table):
    '''
    return a dict
    model_stats = {
        "model": {
            "name": "s32",
            "version": 1,
            "duration": 90,
            "train_window": 60,
            "predict_window": 10
        },
        "stats": {
            "g_g_m": [
                0.32095959595959594,
                0.4668649491714752
            ],
            "g_g_f": [
                0.3654040404040404,
                0.4815635452904544
            ],
            "g_g_x": [
                0.31363636363636366,
                0.46398999646418304
            ],
    '''
    command = """
            SELECT * FROM {}
            """.format(model_stat_table)
    df = hive_context.sql(command)
    rows = df.collect()
    if len(rows) != 1:
        raise Exception('Bad model stat table {} '.format(model_stat_table))
    model_info = rows[0]['model_info']
    model_stats = rows[0]['stats']
    result = {
        'model': model_info,
        'stats': model_stats
    }
    return result


def predict_daily_uckey(sample, days, serving_url, model_stats, columns):

    def _denoise(ts):
        non_zero_ts = [_ for _ in ts if _ != 0]
        nonzero_p = 0.0
        if len(non_zero_ts) > 0:
            nonzero_p = 1.0 * sum(ts) / len(non_zero_ts)

        return [i if i > (nonzero_p / 10.0) else 0 for i in ts]

    def _helper(cols):
        day_list = days[:]
        ucdoc_attribute_map = {}
        for feature in columns:
            ucdoc_attribute_map[feature] = cols[feature]

        # determine ts_n and days
        model_input_ts = []

        # -----------------------------------------------------------------------------------------------
        '''
        The following code is in dlpredictor, here ts has a different format

        'ts': [0, 0, 0, 0, 0, 65, 47, 10, 52, 58, 27, 55, 23, 44, 38, 42, 90, 26, 95, 34, 25, 26, 18, 66, 31, 
        0, 38, 26, 30, 49, 35, 61, 0, 55, 23, 44, 35, 33, 22, 25, 28, 72, 25, 15, 29, 29, 9, 32, 18, 20, 70, 
        20, 4, 11, 15, 10, 8, 3, 0, 5, 3, 0, 23, 11, 44, 11, 11, 8, 3, 38, 3, 28, 16, 3, 4, 20, 5, 4, 45, 15, 9, 3, 60, 27, 15, 17, 5, 6, 0, 7, 12, 0],


        # ts = {u'2019-11-02': [u'1:862', u'3:49', u'2:1154'], u'2019-11-03': [u'1:596', u'3:67', u'2:1024']}
        ts = ucdoc_attribute_map['ts'][0]
        price_cat = ucdoc_attribute_map['price_cat']

        for day in day_list:
            imp = 0.0
            if day in ts:
                count_array = ts[day]
                for i in count_array:
                    parts = i.split(':')
                    if parts[0] == price_cat:
                        imp = float(parts[1])
                        break
            model_input_ts.append(imp)

    
        '''
        model_input_ts = ucdoc_attribute_map['ts']
        price_cat = ucdoc_attribute_map['price_cat']

        # --------------------------------------------------------------------------------------------------------

        # remove science 06/21/2021
        # model_input_ts = replace_with_median(model_input_ts)

        model_input_ts = _denoise(model_input_ts)

        ts_n = normalize_ts(model_input_ts)
        ucdoc_attribute_map['ts_n'] = ts_n

        # add page_ix
        page_ix = ucdoc_attribute_map['uckey'] + '-' + price_cat
        ucdoc_attribute_map['page_ix'] = page_ix

        rs_ts, rs_days = dl_daily_forecast(
            serving_url=serving_url, model_stats=model_stats, day_list=day_list, ucdoc_attribute_map=ucdoc_attribute_map)

        # respose = {'2019-11-02': 220.0, '2019-11-03': 305.0}

        response = {}
        for i, day in enumerate(rs_days):
            response[day] = rs_ts[i]
        return response

    return _helper(cols=sample)


def run(cfg, hive_context):

    model_stats = get_model_stats(hive_context, cfg['model_stat_table'])

    # create day_list from yesterday for train_window
    duration = model_stats['model']['duration']
    predict_window = model_stats['model']['predict_window']
    day_list = model_stats['model']['days']
    day_list.sort()

    local = False
    if not local:
        df_trainready = hive_context.sql(
            'SELECT * FROM {} WHERE uckey="native,b6le0s4qo8,4G,g_f,5,CPC,,1156320000" and price_cat="1" '.format(cfg['trainready_table']))
        df_dist = hive_context.sql(
            'SELECT * FROM {} WHERE ratio=1'.format(cfg['dist_table']))
        df = df_trainready.join(
            df_dist, on=['uckey', 'price_cat'], how='inner')
        columns = df.columns
        samples = df.take(cfg['max_calls'])

    errs = []
    for _ in samples:
        sample = {}
        for feature in columns:
            sample[feature] = _[feature]

        sample['ts'] = sample['ts'][:]

        response = predict_daily_uckey(
            sample=sample, days=day_list, serving_url=cfg['serving_url'], model_stats=model_stats, columns=columns)
        predicted = [response[_] for _ in sorted(response)]

        print(predicted)


if __name__ == '__main__':

    cfg = {
        'log_level': 'warn',
        'trainready_table': 'dlpm_111021_no_residency_no_mapping_trainready_test_12212021',
        'dist_table': 'dlpm_111021_no_residency_no_mapping_tmp_distribution_test_12212021',
        'serving_url': 'http://10.193.217.126:8503/v1/models/dl_test_1221:predict',
        'max_calls': 4,
        'model_stat_table': 'dlpm_111021_no_residency_no_mapping_model_stat_test_12212021',
        'yesterday': 'WILL BE SET IN PROGRAM'}

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    run(cfg=cfg, hive_context=hive_context)
