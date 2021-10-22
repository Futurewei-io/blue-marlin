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

"""
This program tests the system errors of the dl predictor from system query level.
Load the preditions based on date range and attributes from es and fact data to compare.
Suggest to use the test_dlpredictor_system_errors.py to run the testcases for dl predictor.
"""
# Baohua Cao.

from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StringType, IntegerType, MapType
import logging
import yaml
import argparse
import sys
from datetime import datetime, timedelta


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
        day_predicted_traffic = int(
            day_predicted_result['day']['value']) if day_predicted_result['day']['value'] else 0
        predicted_traffic += day_predicted_traffic
        day = day + timedelta(days=1)

    return predicted_traffic


def _list_to_map(count_array):
    count_map = {}
    for item in count_array:
        key_value = item.split(':')
        count_map[key_value[0]] = key_value[1]
    return count_map


def add_count_map(df):
    # Convert count_array to count_map
    list_to_map_udf = udf(_list_to_map, MapType(
        StringType(), StringType(), False))
    df = df.withColumn('count_map', list_to_map_udf(df.count_array))
    return df


def load_factdata(sc, cfg, starting_day, ending_day, attributes_condition):
    hive_context = HiveContext(sc)
    bucket_id_max = cfg['bucket_id_max']
    # step 1: load the original fact data.
    # step 2: load the distribution e.g. 133904 uckeys.
    # step 3: inner join the original fact data with the distribution. e.g. 133904 uckeys.
    # step 4: filter the new fact data with date range e.g. 2020-01-30 - 2020-02-08, 10 days.
    # step 5: filter the new fact data with conditions.

    # step 1: load the original fact data
    command = """select uckey, day, hour, count_array from {} where bucket_id <= {} 
              """.format(cfg['factdata'], bucket_id_max)
    df = hive_context.sql(command)
    df = add_count_map(df)
    # [Row(count_array=['3:4'], day='2019-11-02', hour=19, uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', count_map={'3': '4'})]

    # Explode count_map to have pcat and count on separate columns
    df = df.select('uckey', 'day', 'hour', explode(df.count_map)).withColumnRenamed(
        "key", "price_cat").withColumnRenamed("value", "count")
    # [Row(uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', day='2019-11-02', hour=19, price_cat='3', count='4')]

    # This is to have the fact data uckey-price_cat pair based on daily count to join the distribution.
    df = df.groupBy('uckey', 'day', 'price_cat').agg({"count": "sum"}).withColumnRenamed("sum(count)", "count")
    # [Row(uckey='splash,5cd1c663263511e6af7500163e291137,WIFI,g_m,4,CPT,3,', day='2019-11-02', price_cat='1', count=56.0)]

    # step 2: load the distribution e.g. 133904 uckeys
    command = 'select uckey, price_cat from {} where ratio > 0'.format(
        cfg['distribution'])
    dfd = hive_context.sql(command)

    # step 3: inner join the original fact data with the distribution #distinct uckeys in joined fact data: e.g. 133904
    df = df.join(dfd, [df.uckey == dfd.uckey, df.price_cat ==
                       dfd.price_cat], how="inner").drop(dfd.uckey).drop(dfd.price_cat)
    # e.g. df.select(df.uckey).distinct().count(): 133904

    # step 4: filter the new fact data with date range e.g. 2020-01-30 - 2020-02-08, 10 days
    df = df.filter((df.day <= ending_day) & (df.day >= starting_day))
    # e.g. df.count(): 15152287, df.select(df.uckey).distinct().count(): 92612

    # step 5: filter the new fact data with conditions.
    uckey_attrs = cfg['uckey_attrs']
    for attr_index in range(len(uckey_attrs)):
        df = df.withColumn(uckey_attrs[attr_index], udf(
            lambda x: x.split(',')[attr_index], StringType())(df.uckey))
    # e.g. [Row(uckey=u'magazinelock,01,2G,,,CPM,13', day=u'2020-01-19', hour=8, count_array=[u'1:10'],
    # m=u'magazinelock', si=u'01', t=u'2G', g=u'', a=u'', pm=u'CPM', r=u'13')]
    if attributes_condition:
        for attr, attr_value in attributes_condition.items():
            df = df.filter(df[attr] == str(attr_value))

    return df


def calculate_factdata_traffic(df_factdata):
    df_traffic = df_factdata.agg({"count": "sum"}).withColumnRenamed(
        "sum(count)", "sum_traffic")
    sum_traffic = int(df_traffic.take(1)[0]["sum_traffic"])

    return sum_traffic


def calculate_error(factdata_traffic, predicted_traffic):
    mape_error = 1.0 * abs(factdata_traffic - predicted_traffic) / \
        factdata_traffic if factdata_traffic else 0
    smape_error = 2.0 * abs(factdata_traffic - predicted_traffic) / \
        (factdata_traffic + predicted_traffic) if factdata_traffic else 0
    mape_error = "%.2f" % round(mape_error, 2)
    smape_error = "%.2f" % round(smape_error, 2)
    return mape_error, smape_error


def run_testcase(sc, cfg, starting_day, ending_day, attributes_condition):
    # step 1: load the related factdata.
    # step 2: calculate the factdata's traffic.
    # step 3: calculate the es predictions' traffic.
    # step 4: compare the two traffic and output the error result.
    df_factdata = load_factdata(
        sc, cfg, starting_day, ending_day, attributes_condition)
    factdata_traffic = calculate_factdata_traffic(df_factdata)
    predicted_traffic = query_predictions_traffic(
        cfg, starting_day, ending_day, attributes_condition)
    mape_error, smape_error = calculate_error(
        factdata_traffic, predicted_traffic)

    return [starting_day, ending_day, str(attributes_condition), str(factdata_traffic),
            str(predicted_traffic), str(mape_error), str(smape_error)]


def run(cfg):
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    try:
        res = [['starting_day', 'ending_day', 'attributes_condition',
                'factdata_traffic', 'predicted_traffic', 'mape_error', 'smape_error']]
        starting_day, ending_day = "2020-01-30", "2020-02-08"
        # r: 40 - beijing, r: 49 - shenyang, r: 80 - cities group
        attributes_conditions = [None, {"g": "g_m"}, {"g": "g_f"}, {"a": "4"}, {"a": "6"}, {"g": "g_m", "a": "4"},
                                 {"g": "g_f", "a": "4"}, {"g": "g_f", "a": "6"}, {
                                     "r": "40"}, {"r": "49"},
                                 {"r": "80"}, {"m": "native"}, {"pm": "CPC"},
                                 {"g": "g_f", "a": "6", "m": "native"},  {
                                     "g": "g_f", "a": "6", "m": "native", "r": "40"}
                                 ]
        for attributes_condition in attributes_conditions:
            print('processing ' + str((starting_day, ending_day)) +
                  ', ' + str(attributes_condition))
            testcase_result = run_testcase(
                sc, cfg, starting_day, ending_day, attributes_condition)
            res.append(testcase_result)
    finally:
        sc.stop()

    for resi in res:
        print(", ".join(resi))


# spark-submit --master yarn --num-executors 20 --executor-cores 5 --jars lib/elasticsearch-hadoop-6.5.2.jar tests/test_dlpredictor_system_errors_0 tests/conf/config.yml
if __name__ == '__main__':
    logger = logging.getLogger('test-system-errors-dlpredictor-logger')
    parser = argparse.ArgumentParser(
        description='test system errors of dlpredictor')
    parser.add_argument('config_file')
    args = parser.parse_args()

    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            logger.info("Successfully open {}".format(args.config_file))
    except IOError as e:
        logger.error(
            "Open config file unexpected error: I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        logger.error("Unexpected error:{}".format(sys.exc_info()[0]))
        raise

    run(cfg)
