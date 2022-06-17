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
This file calculated the actual and predicted traffic a si.


1. Input is a SI
2. Output is
    actual traffic from ts (no join with dist_table, all the uckeys are participating)
    predicted traffic from ES

spark-submit --master yarn --py-files lib/imscommon-2.0.0-py2.7.egg --num-executors 5 --executor-cores 3 --executor-memory 16G --driver-memory 16G tests/si_traffic_prediction_ckeck/si_traffic_prediction_check_4.py


+------------------------------+----------+----------+--------+---------+-----+
|version                       |si        |day       |actual  |predicted|error|
+------------------------------+----------+----------+--------+---------+-----+
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-22|68608008|74065399 |8%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-23|69360585|75582893 |9%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-24|69407841|70786481 |2%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-25|66652553|73027064 |9%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-26|65385719|73890032 |13%  |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-27|69777146|73531806 |5%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-28|70173316|73850899 |5%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-29|69984013|76397722 |9%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-30|70183372|75982843 |8%   |
|110421_no_residency_no_mapping|a47eavw7ex|2021-07-31|67949880|73744501 |8%   |
+------------------------------+----------+----------+--------+---------+-----+



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

DATE_FORMATE = '%Y%m%d'

def query_predictions_traffic(cfg, starting_day, ending_day, si):
    es_host, es_port = cfg['es_host'], cfg['es_port']
    es_index, es_type = cfg['es_predictions_index'], cfg['es_predictions_type']
    es = Elasticsearch([{'host': es_host, 'port': es_port}])
    day = datetime.strptime(starting_day, DATE_FORMATE)
    end_date = datetime.strptime(ending_day, DATE_FORMATE)
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
                        "field": "ucdoc.predictions.{}.hours.total".format(day.strftime(DATE_FORMATE))
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


def run(hive_context, cfg, version, traffic, target_days):

    command = """
        SELECT
        T1.uckey,
        T1.price_cat,
        T1.si,
        T1.ts,
        T1.ts_ver 
        FROM {} AS T1 
        WHERE T1.si='{}'
        """.format(cfg['ts_table'],  traffic['si'])

    print(command)

    df = hive_context.sql(command)
    df.cache()

    # The last days of the ts are ['2020-05-27', '2020-05-28', '2020-05-29', '2020-05-30', '2020-05-31'] which are real values
    rows = df.collect()

    # Actual Value
    real_traffic = [0 for _ in range(len(target_days))]

    shift = 1
    for row in rows:
        ts_ver = row['ts_ver']
        ts_ver = ts_ver[:-2]
        if len(target_days) != len(ts_ver):
            raise Exception('ts_ver len is not equal to target_days')
        for i in range(len(target_days)):
            if ts_ver[i]:
                real_traffic[i] += ts_ver[i]

    # Predicted Value
    predicted_traffic = []
    for day in target_days:
        v = query_predictions_traffic(cfg=cfg, starting_day=day, ending_day=day, si=si)
        predicted_traffic.append(v)

    errors = []
    for i in range(len(target_days)):
        error = c_error(real_traffic[i], predicted_traffic[i])
        errors.append(round(100*error, 3))

    # this is only one records
    schema = ["version", "si", "day", "actual", "predicted", "error"]
    data = []
    for i in range(len(target_days)):
        data.append((version, si, target_days[i], real_traffic[i], predicted_traffic[i], errors[i]))
    df = hive_context.createDataFrame(data, schema)

    #df.show(10, False)
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
        'ts_table': 'dlpm_05172022_tmp_ts',
        'uckey_attrs': ['m', 'si', 't', 'g', 'a', 'pm', 'r', 'ipl'],
        'es_host': '10.213.37.41',
        'es_port': '9200',
        'es_predictions_index': 'dlpredictor_06092022_predictions',
        'es_predictions_type': 'doc',
        'report_table': 'si_traffic_prediction_check_05172022_06092022'
    }

    # list of days in ts_ver in ts table.
    target_days = sorted(["20220321", "20220322", "20220323", "20220324", "20220325", "20220326", "20220327", "20220328"])

    VERSION = '05172022_06092022'
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
    
    #sis = sis[:2]

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    for si in sis:
        traffic['si'] = si
        run(hive_context=hive_context, cfg=cfg, version=VERSION, traffic=traffic, target_days=target_days)
        print(si)

    sc.stop()
