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
This file is to test the accuracy rate of the prediction.

1. Input is a SI
2. From distribution tmp, it gets a list of non-dense and dense uskeys for SI
3. For each group of uckeys, it calculates real-value and predicted-value

spark-submit --master yarn --py-files dist/imscommon-2.0.0-py2.7.egg --num-executors 5 --executor-cores 3 --executor-memory 16G --driver-memory 16G tests/test_dlpredictor_system_errors_4.py

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


def query_predictions_traffic(cfg, starting_day, ending_day, uckeys):
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
                        "field": "ucdoc.predictions.{}.hours.h1".format(day.strftime('%Y-%m-%d'))
                    }
                }
            }
        }
        if uckeys:
            for uckey in uckeys:
                match = {
                    "term": {
                        "ucdoc.uckey.keyword": uckey
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


def __save_as_table(df, table_name, hive_context, create_table):
    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

    command = """
            CREATE TABLE IF NOT EXISTS {}
            (
            report string
            )
            """.format(table_name)

    hive_context.sql(command)

    df.select('report').write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)


def add_l2_to_l1(l1, l2):
    for i in range(len(l2)):
        if l2[i] is None:
            continue
        l1[i] += l2[i]


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
        WHERE T1.si='{}' AND T1.price_cat='{}'
        """.format(cfg['ts_table'], cfg['dist_table'], traffic['si'], traffic['price_cat'])

    print(command)

    df = hive_context.sql(command)
    df.cache()

    # The last days of the ts are ['2020-05-27', '2020-05-28', '2020-05-29', '2020-05-30', '2020-05-31'] which are real values
    dense_uckeys = df.where('ratio=1').collect()
    non_dense_uckeys = df.where('ratio!=1').collect()

    # Actual Value
    real_traffic_dense = {}
    real_traffic_non_dense = {}

    for item in dense_uckeys:
        ts = item['ts'][-5:]
        uckey = item['uckey']
        price_cat = item['price_cat']
        real_traffic_dense[(uckey, price_cat)] = ts

    for item in non_dense_uckeys:
        ts = item['ts'][-5:]
        uckey = item['uckey']
        price_cat = item['price_cat']
        real_traffic_non_dense[(uckey, price_cat)] = ts

    # Predicted Value

    predicted_traffic_dense = {}
    predicted_traffic_non_dense = {}

    for key, ts in real_traffic_dense.items():
        uckey = key[0]
        price_cat = key[1]
        i = -1
        pts = []
        for day in target_days:
            i += 1
            if ts[i] == 0:
                # Here is for masking, if actual value is 0 then predicted value is masked (not be considiered in error calculation)
                pts.append(0)
            else:
                v = query_predictions_traffic(cfg=cfg, starting_day=day, ending_day=day, uckeys=[uckey])
                pts.append(v)

        predicted_traffic_dense[key] = pts

    for key, ts in real_traffic_non_dense.items():
        uckey = key[0]
        price_cat = key[1]
        i = -1
        pts = []
        for day in target_days:
            i += 1
            if ts[i] == 0:
                # Here is for masking, if actual value is 0 then predicted value is masked (not be considiered in error calculation)
                pts.append(0)
            else:
                v = query_predictions_traffic(cfg=cfg, starting_day=day, ending_day=day, uckeys=[uckey])
                pts.append(v)

        predicted_traffic_non_dense[key] = pts

    print_result(real_traffic_dense,"Actual DENSE")
    print_result(predicted_traffic_dense,"Predicted DENSE")

    print_result(real_traffic_non_dense,"Actual NON-DENSE")   
    print_result(predicted_traffic_non_dense,"Predicted NON-DENSE")



def print_result(mdict, title):
    tmp = [0 for _ in range(len(target_days))]
    for _, ts in mdict.items():
        add_l2_to_l1(tmp, ts)
    print(title + " --> " + str(tmp))


if __name__ == "__main__":

    cfg = {
        'file_name': 'test_dlpredictor_system_errors_4',
        'log_level': 'WARN',
        'ts_table': 'dlpm_03182021_tmp_ts',
        'dist_table': 'dlpm_03182021_tmp_distribution',
        'uckey_attrs': ['m', 'si', 't', 'g', 'a', 'pm', 'r', 'ipl'],
        'es_host': '10.213.37.41',
        'es_port': '9200',
        'es_predictions_index': 'dlpredictor_05062021_predictions',
        'es_predictions_type': 'doc',
        'report_table': 'dlpredictor_report'
    }

    target_days = sorted(['2020-05-27', '2020-05-28', '2020-05-29', '2020-05-30', '2020-05-31'])

    #This is match with es query.Don't change it.
    PRICE_CAT = '1'

    traffic = {'si': 'd4d7362e879511e5bdec00163e291137', 'price_cat': PRICE_CAT}

    sis = [
        'a47eavw7ex',
        '68bcd2720e5011e79bc8fa163e05184e',
        '66bcd2720e5011e79bc8fa163e05184e',
        '72bcd2720e5011e79bc8fa163e05184e',
        'f1iprgyl13',
        'q4jtehrqn2',
        'm1040xexan',
        'd971z9825e',
        'a290af82884e11e5bdec00163e291137',
        'w9fmyd5r0i',
        'x2fpfbm8rt',
        'e351de37263311e6af7500163e291137',
        'k4werqx13k',
        '5cd1c663263511e6af7500163e291137',
        '17dd6d8098bf11e5bdec00163e291137',
        'd4d7362e879511e5bdec00163e291137',
        '15e9ddce941b11e5bdec00163e291137']

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    for si in sis:
        traffic['si'] = si
        run(sc=sc, hive_context=hive_context, cfg=cfg, traffic=traffic, target_days=target_days)
        print("---------------------------------------------------------")

    sc.stop()
