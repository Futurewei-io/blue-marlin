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
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list
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


def run(sc, hive_context, cfg, traffic, target_days):

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
    cfg_json = json.dumps(cfg)

    df = hive_context.createDataFrame([(traffic_json,
                                        cfg_json,
                                        error,
                                        real_traffic,
                                        predicted_traffic
                                        )], ["Traffic", "Config", "Error", "Actual", "Predicted"])

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
        'pre_cluster_table': 'dlpm_06242021_1635_tmp_pre_cluster',
        'dist_table': 'dlpm_06242021_1635_tmp_distribution',
        'uckey_attrs': ['m', 'si', 't', 'g', 'a', 'pm', 'r', 'ipl'],
        'es_host': '10.213.37.41',
        'es_port': '9200',
        'es_predictions_index': 'dlpredictor_06242021_1635_predictions',
        'es_predictions_type': 'doc',
        'report_table': 'si_only_traffic_prediction_check'
    }

    target_days = sorted(['2020-06-21', '2020-06-22', '2020-06-23', '2020-06-24', '2020-06-25', '2020-06-26', '2020-06-27', '2020-06-28', '2020-06-29', '2020-06-30'])

    traffic = {'si': 'd4d7362e879511e5bdec00163e291137', 'version': '04072021-01'}

    sis = [
        '66bcd2720e5011e79bc8fa163e05184e',
        '7b0d7b55ab0c11e68b7900163e3e481d',
        'a8syykhszz',
        'w3wx3nv9ow5i97',
        'x2fpfbm8rt',
        '17dd6d8098bf11e5bdec00163e291137',
        '5cd1c663263511e6af7500163e291137',
        '68bcd2720e5011e79bc8fa163e05184e',
        '71bcd2720e5011e79bc8fa163e05184e',
        'a290af82884e11e5bdec00163e291137',
        'a47eavw7ex',
        'b6le0s4qo8',
        'd4d7362e879511e5bdec00163e291137',
        'd971z9825e',
        'd9jucwkpr3',
        'e351de37263311e6af7500163e291137',
        'f1iprgyl13',
        'j1430itab9wj3b',
        'k4werqx13k',
        'l03493p0r3',
        'l2d4ec6csv',
        'p7gsrebd4m',
        's4z85pd1h8',
        'w9fmyd5r0i',
        'x0ej5xhk60kjwq',
        'z041bf6g4s']

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    for si in sis:
        traffic['si'] = si
        run(sc=sc, hive_context=hive_context, cfg=cfg, traffic=traffic, target_days=target_days)
        print(si)

    sc.stop()
