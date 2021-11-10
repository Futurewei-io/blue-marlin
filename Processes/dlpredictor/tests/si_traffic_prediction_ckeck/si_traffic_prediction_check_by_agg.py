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


from imscommon.es.ims_esclient import ESClient
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, sum as fsum
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType
import math
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json

"""
This file calculated the actual and predicted dense/non-dense traffic a si.


1. Input is a SI
2. Output is
    actual-dense traffic
    predicted-dense traffic
    actual-non-dense traffic
    predicted-non-dense traffic

From distribution tmp, it gets a list of non-dense and dense uskeys for SI
For each group of uckeys, it calculates real-value and predicted-value

spark-submit --master yarn --py-files lib/imscommon-2.0.0-py2.7.egg --num-executors 5 --executor-cores 3 --executor-memory 16G --driver-memory 16G tests/si_traffic_prediction_ckeck/si_traffic_prediction_check_by_agg.py

Author: Reza
"""

"""
 attr = uckey.split(',')
    attributes_condition = {
        'm': attr[0],
        'si': attr[1],
        't': attr[2],
        'g': attr[3],
        'a': attr[4],
        'pm': attr[5],
        'r': attr[6],
        'ipl': attr[7]
    }
"""


def get_total_traffic(hive_context):
    command = """
        SELECT
        T1.imp,
        T1.si 
        FROM {} AS T1
        """.format(cfg['pre_cluster_table'])

    df = hive_context.sql(command)
    result = df.agg(fsum('imp')).take(1)[0]['sum(imp)']
    return result


def get_total_dense_traffic(hive_context):
    command = """
        SELECT
        T1.imp,
        T1.si,
        T1.sparse 
        FROM {} AS T1 WHERE T1.sparse=false
        """.format(cfg['pre_cluster_table'])

    df = hive_context.sql(command)
    result = df.agg(fsum('imp')).take(1)[0]['sum(imp)']
    return result


def get_uckey_price_count(hive_context):
    command = """
        SELECT
        T1.imp,
        T1.si 
        FROM {} AS T1
        """.format(cfg['pre_cluster_table'])

    df = hive_context.sql(command)
    result = df.count()
    return result


def query_predictions_traffic(cfg, starting_day, ending_day, si):
    es_host, es_port = cfg['es_host'], cfg['es_port']
    es_index, es_type = cfg['es_predictions_index'], cfg['es_predictions_type']
    es = Elasticsearch([{'host': es_host, 'port': es_port}])
    day = datetime.strptime(starting_day, '%Y-%m-%d')
    end_date = datetime.strptime(ending_day, '%Y-%m-%d')
    predicted_traffic = 0
    # sum up the daily traffic as the total predicted traffic.
    while day <= end_date:
        prediction_day_query = {
            "size": 0,
            "query": {
                "bool": {
                    "should": [
                    ]
                }
            },
            "aggs": {
                "day": {
                    "sum": {
                        "field": "ucdoc.predictions.{}.hours.total".format(day.strftime('%Y-%m-%d'))
                    }
                }
            }
        }

        match = {
            "match": {
                "ucdoc.si": si
            }
        }
        prediction_day_query['query']['bool']['should'].append(match)
        res = es.search(index=es_index, body=prediction_day_query)
        """
        result sample:
        "hits" : {
            "total" : 1339040,
            "max_score" : 0.0,
            "hits" : [ ]
        },
        "aggregations" : {
            "day" : {
                "value" : 17131.516769555536
                }
        }"""
        day_predicted_result = res['aggregations']
        day_predicted_traffic = int(day_predicted_result['day']['value']) if day_predicted_result['day']['value'] else 0
        predicted_traffic += day_predicted_traffic
        day = day + timedelta(days=1)

    return predicted_traffic


def table_exists(table_name, hive_context):
    command = """
            SHOW TABLES LIKE '{}'
            """.format(table_name)

    df = hive_context.sql(command)
    return df.count() > 0


def __save_as_table(df, table_name, hive_context):

    if not table_exists(table_name, hive_context):

        df.createOrReplaceTempView("r900_temp_table")
        command = """
            CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM r900_temp_table
            """.format(table_name)
        hive_context.sql(command)
    else:
        df.write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)


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


def get_si_traffic(cfg, hive_context, si):
    command = """
        SELECT
        T1.imp,
        T1.si 
        FROM {} AS T1 
        WHERE T1.si='{}'
        """.format(cfg['pre_cluster_table'], si)

    df = hive_context.sql(command)
    result = df.agg(fsum('imp')).take(1)[0]['sum(imp)']
    return result


def get_si_dense_traffic(cfg, hive_context, si):
    command = """
        SELECT
        T1.imp,
        T1.si,
        T1.sparse  
        FROM {} AS T1 
        WHERE T1.si='{}' AND T1.sparse=false 
        """.format(cfg['pre_cluster_table'], si)

    df = hive_context.sql(command)
    result = df.agg(fsum('imp')).take(1)[0]['sum(imp)']
    return result


def get_si_uckey_price_count(cfg, hive_context, si):
    command = """
        SELECT
        T1.imp,
        T1.si 
        FROM {} AS T1 
        WHERE T1.si='{}'
        """.format(cfg['pre_cluster_table'], si)

    df = hive_context.sql(command)
    result = df.count()
    return result


def run(sc, hive_context, cfg, version, traffic, target_days, _agg):

    command = """
        SELECT
        T1.uckey,
        T1.price_cat,
        T1.si,
        T1.ts,
        T2.ratio 
        FROM {} AS T1 INNER JOIN {} AS T2 
        ON T1.uckey=T2.uckey AND T1.price_cat=T2.price_cat 
        WHERE T1.si='{}'
        """.format(cfg['pre_cluster_table'], cfg['dist_table'],  traffic['si'])

    print(command)

    df = hive_context.sql(command)
    df.cache()

    # The last days of the ts are ['2020-05-27', '2020-05-28', '2020-05-29', '2020-05-30', '2020-05-31'] which are real values
    uckeys = df.collect()

    # Actual Value
    real_traffic = {}
    shift = len(target_days)

    for item in uckeys:
        ts = item['ts'][-shift:]
        uckey = item['uckey']
        if uckey not in real_traffic:
            real_traffic[uckey] = ts
        else:
            old_ts = real_traffic[uckey]
            new_ts = []
            for i in range(len(ts)):
                v = ts[i] + old_ts[i]
                new_ts.append(v)
            real_traffic[uckey] = new_ts

    real_traffic = agg_uckeys(real_traffic, target_days)

    # Predicted Value
    predicted_traffic = []
    for day in target_days:
        v = query_predictions_traffic(cfg=cfg, starting_day=day, ending_day=day, si=si)
        predicted_traffic.append(v)

    error = error_m(real_traffic, predicted_traffic)[0]

    traffic_json = json.dumps(traffic)
    cfg_json = json.dumps({})

    # here are the other statistical information
    si_traffic = get_si_traffic(cfg, hive_context, si)
    si_uckey_price_count = get_si_uckey_price_count(cfg, hive_context, si)

    w1_error = si_traffic * 1.0 / _agg['total_traffic'] * error
    w2_error = si_uckey_price_count * 1.0 / _agg['uckey_price_count'] * error
    _agg['total_w1_error'] += w1_error
    _agg['total_w2_error'] += w2_error

    si_dense_traffic = get_si_dense_traffic(cfg, hive_context, si)
    dense_ratio = si_dense_traffic * 1.0 / si_traffic

    # this is only one records
    df = hive_context.createDataFrame([(version,
                                        si,
                                        si_traffic, si_uckey_price_count, w1_error, w2_error, error, dense_ratio,
                                        cfg_json, real_traffic, predicted_traffic
                                        )], ["version", "si",
                                             "si_traffic", "si_uckey_price_count", "w1_error", "w2_error", "error", "dense_ratio",
                                             "config", "actual", "predicted"])

    __save_as_table(df, cfg['report_table'], hive_context)


def add_l2_to_l1(l1, l2):
    for i in range(len(l2)):
        if l2[i] is None:
            continue
        l1[i] += l2[i]


def agg_uckeys(mdict, _days):
    tmp = [0 for _ in range(len(_days))]
    for _, ts in mdict.items():
        add_l2_to_l1(tmp, ts)
    return tmp


if __name__ == "__main__":

    cfg = {
        'log_level': 'WARN',
        'pre_cluster_table': 'dlpm_110221_no_residency_no_mapping_tmp_pre_cluster',
        'dist_table': 'dlpm_110221_no_residency_no_mapping_tmp_distribution',
        'uckey_attrs': ['m', 'si', 't', 'g', 'a', 'pm', 'r', 'ipl'],
        'es_host': '10.213.37.41',
        'es_port': '9200',
        'es_predictions_index': 'dlpredictor_110221_no_residency_no_mapping_predictions',
        'es_predictions_type': 'doc',
        'report_table': 'si_only_traffic_prediction_check_v3'
    }

    _agg = {}

    # list of last days in dataset, use model-stat to get the days
    target_days = sorted(["2021-07-12", "2021-07-13", "2021-07-14", "2021-07-15", "2021-07-16","2021-07-17", "2021-07-18", "2021-07-19", "2021-07-20", "2021-07-21"])

    VERSION = '110221_no_residency_no_mapping_2'
    traffic = {'si': '', 'version': VERSION}
 
    sis = [
        'a47eavw7ex',
'66bcd2720e5011e79bc8fa163e05184e',
'x0ej5xhk60kjwq',
'l03493p0r3',
'7b0d7b55ab0c11e68b7900163e3e481d',
'b6le0s4qo8',
'e351de37263311e6af7500163e291137',
'a290af82884e11e5bdec00163e291137',
'68bcd2720e5011e79bc8fa163e05184e',
'f1iprgyl13',
'w9fmyd5r0i',
'w3wx3nv9ow5i97',
'd971z9825e',
'l2d4ec6csv',
'z041bf6g4s',
'71bcd2720e5011e79bc8fa163e05184e',
'5cd1c663263511e6af7500163e291137',
'x2fpfbm8rt',
'd9jucwkpr3',
'k4werqx13k',
'j1430itab9wj3b',
'a8syykhszz',
's4z85pd1h8',
'17dd6d8098bf11e5bdec00163e291137',
'd4d7362e879511e5bdec00163e291137']

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    _agg['total_traffic'] = get_total_traffic(hive_context)
    _agg['total_dense_traffic'] = get_total_dense_traffic(hive_context)
    _agg['uckey_price_count'] = get_uckey_price_count(hive_context)
    _agg['total_w1_error'] = 0.0
    _agg['total_w2_error'] = 0.0
    dense_ratio = _agg['total_dense_traffic'] * 1.0 / _agg['total_traffic']

    for si in sis:
        traffic['si'] = si
        run(sc=sc, hive_context=hive_context, cfg=cfg, version=VERSION, traffic=traffic, target_days=target_days, _agg=_agg)
        print(si)

    # this is only one records
    df = hive_context.createDataFrame([(VERSION,
                                        'all',
                                        _agg['total_traffic'], _agg['uckey_price_count'], _agg['total_w1_error'], _agg['total_w2_error'], -1.0, dense_ratio,
                                        json.dumps(cfg), [-1], [-1]
                                        )], ["version", "si",
                                             "si_traffic", "si_uckey_price_count", "w1_error", "w2_error", "error", "dense_ratio",
                                             "config", "actual", "predicted"])

    # print([(VERSION,
    #         'all',
    #         _agg['total_traffic'], _agg['uckey_price_count'], _agg['total_w1_error'], _agg['total_w2_error'], -1.0, dense_ratio,
    #         json.dumps(cfg), [-1], [-1]
    #         )])

    __save_as_table(df, cfg['report_table'], hive_context)

    sc.stop()
