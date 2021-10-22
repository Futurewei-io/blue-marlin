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
This file is to test the accuracy rate of the prediction for a list of queries(traffics) for a day.
The assumption is that es prediction and factdata has intersections on days.

This program reads the predeicted values from ES and calcualte the MAPE Error based on their real values from factdata.

spark-submit --master yarn --py-files dist/imscommon-2.0.0-py2.7.egg --num-executors 20 --executor-cores 3 --executor-memory 16G --driver-memory 16G tests/test_dlpredictor_system_errors_3.py

Author: Reza
"""


def sum_count_array(count_array):
    '''
    ["2:28","1:15"]
    '''
    count = 0
    for item in count_array:
        _, value = item.split(':')
        count += int(value)

    return count


def get_factdata_imp_for_traffic(sc, hive_context, cfg, traffic, target_days):

    # Reading the max bucket_id
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata = cfg['factdata_table']

    start_bucket = 0
    total_count = {}
    for day in target_days:
        total_count[day] = 0

    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = """
        SELECT
        FACTDATA.count_array,
        FACTDATA.day,
        FACTDATA.hour,
        FACTDATA.uckey
        FROM {} AS FACTDATA
        WHERE FACTDATA.bucket_id BETWEEN {} AND {} 
        AND FACTDATA.day BETWEEN '{}' AND '{}'
        """.format(factdata, str(start_bucket), str(end_bucket), target_days[0], target_days[-1])

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)

        # [Row(count_array=[u'1:504'], day=u'2019-11-02', hour=2, uckey=u'magazinelock,04,WIFI,g_m,1,CPM,78'
        # Extract traffic attributes from uckey based on arritbutes in traffic
        uckey_attrs = cfg['uckey_attrs']
        for attr_index in range(len(uckey_attrs)):
            if (uckey_attrs[attr_index] in traffic):
                df = df.withColumn(uckey_attrs[attr_index], udf(lambda x: x.split(',')[attr_index], StringType())(df.uckey))

        # e.g. [Row(uckey=u'magazinelock,01,2G,,,CPM,13', ...
        # m=u'magazinelock', si=u'01', t=u'2G', g=u'', a=u'', pm=u'CPM', r=u'13')]
        # Filter uckey based on traffic
        for attr, attr_value in traffic.items():
            df = df.filter(df[attr] == str(attr_value))

        # Extract imp-count from count_array
        # [Row(uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', day='2019-11-02', hour=19, count_array=[u'1:504'])]
        df = df.withColumn('count', udf(sum_count_array, IntegerType())(df.count_array))

        l = df.groupBy('day').agg({'count': 'sum'}).collect()
        for item in l:
            _day = item['day']
            _count = item['sum(count)']
            total_count[_day] += _count

    return total_count


def get_predictions_for_traffic(cfg, traffic, target_days):
    total_count = {}
    for day in target_days:
        total_count[day] = 0

    for target_day in target_days:
        count = query_predictions_traffic(cfg=cfg, starting_day=target_day, ending_day=target_day, attributes_condition=traffic)
        total_count[target_day] = count

    return total_count

# query total traffic of precictions those matched the conditions.


def query_predictions_traffic(cfg, starting_day, ending_day, attributes_condition):
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
                    "must": [
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
        if attributes_condition:
            for attr, attr_value in attributes_condition.items():
                match = {
                    "match": {
                        "ucdoc." + attr: attr_value
                    }
                }
                prediction_day_query['query']['bool']['must'].append(match)
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


def run(sc, hive_context, cfg, traffic, target_days):

    factdata_imp = get_factdata_imp_for_traffic(sc=sc, hive_context=hive_context, cfg=cfg, traffic=traffic, target_days=target_days)
    predicted_imp = get_predictions_for_traffic(cfg=cfg, traffic=traffic, target_days=target_days)

    report = {'real_imp': factdata_imp,
              'predicted_imp': predicted_imp,
              'cfg': cfg,
              'traffic': traffic,
              'days': target_days
              }

    report = json.dumps(report)

    df = hive_context.createDataFrame([(report,)], ['report'])

    __save_as_table(df, cfg['report_table'], hive_context, False)


if __name__ == "__main__":

    cfg = {
        'log_level': 'WARN',
        'factdata_table': 'dlpm_03182021_tmp_area_map',
        'bucket_size': 10,
        'bucket_step': 2,
        'uckey_attrs': ['m', 'si', 't', 'g', 'a', 'pm', 'r', 'ipl'],
        'es_host': '10.213.37.41',
        'es_port': '9200',
        'es_predictions_index': 'dlpredictor_05052021_predictions',
        'es_predictions_type': 'doc',
        'report_table': 'dlpredictor_report'
    }

    target_days = sorted(['2020-06-01', '2020-06-02', '2020-06-03', '2020-06-04', '2020-06-05','2020-06-06', '2020-06-07', '2020-06-08', '2020-06-09', '2020-06-10'])
    traffic = {'si': 'd4d7362e879511e5bdec00163e291137'}

    sis = [
    '06',
    '11',
    '05',
    '04',
    '03',
    '02',
    '01',
    'l03493p0r3',
    'x0ej5xhk60kjwq',
    'g7m2zuits8',
    'w3wx3nv9ow5i97',
    'a1nvkhk62q',
    'g9iv6p4sjy',
    'c4n08ku47t',
    'b6le0s4qo8',
    'd9jucwkpr3',
    'p7gsrebd4m',
    'a8syykhszz',
    'l2d4ec6csv',
    'j1430itab9wj3b',
    's4z85pd1h8',
    'z041bf6g4s',
    '71bcd2720e5011e79bc8fa163e05184e',
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

    sc.stop()
