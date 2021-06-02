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
This is a system testing program to test the system errors of the dl predictor.
This program will compare the total traffic of the fact data and the ES predictions.
This program uses date range and different attributes to compare MAEP and SMAPE errors.
"""

import logging
import yaml
import argparse
import sys
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StringType, IntegerType, MapType


def load_es_predictions(cfg):
    es_host, es_port = cfg['es_host'], cfg['es_port']
    es_index, es_type = cfg['es_predictions_index'], cfg['es_predictions_type']
    es = Elasticsearch([{'host': es_host, 'port': es_port}])

    # use scroll api to load big dataset of ES ucdocs without 10000 hits limit.
    resp = es.search(index=es_index, body={"size": 10000}, scroll="20s")
    old_scroll_id = resp['_scroll_id']
    es_ucdocs = []
    ucdocs_scrolled = resp['hits']['hits']
    for ucdoc in ucdocs_scrolled:
        es_ucdocs.append(ucdoc)
    while len(resp['hits']['hits']):
        resp = es.scroll(scroll_id=old_scroll_id, scroll="20s")
        old_scroll_id = resp['_scroll_id']
        ucdocs_scrolled = resp['hits']['hits']
        for ucdoc in ucdocs_scrolled:
            es_ucdocs.append(ucdoc)
        print(str(len(es_ucdocs)) + ' scrolled ucdocs were loaded successfully from the ES.')

    print(str(len(es_ucdocs)) + ' ucdocs in total were loaded finally from the ES.')

    return es_ucdocs


# query total traffic of precictions those matched the conditions with real daily traffic check mask.
# mask is used to check whether the daily traffic prediction is based on real daily traffic.
# mask[uckey][day] = 0 which means this uckey's traffic prediction in this day is ignored in counts.
def calculate_es_predictions_traffic(attributes_condition, es_daily_ucdocs, mask):
    predicted_traffic = 0
    es_uckeys_used = set()

    # es_daily_ucdoc sample:
    # {'a': u'5', 'g': u'g_f', 'm': u'magazinelock', 'daily_count': 16138, 'si': u'01', 'r': u'74',
    # 'uckey': u'magazinelock,01,WIFI,g_f,5,CPM,74', 't': u'WIFI', 'day': '2020-01-30', 'pm': u'CPM'}
    for es_daily_ucdoc in es_daily_ucdocs:
        condition_qualified = True
        # condition_qualified is used to filter the attributes.
        if attributes_condition:
            for attr, attr_value in attributes_condition.items():
                if es_daily_ucdoc[attr] != attr_value:
                    condition_qualified = False
                    break
        if condition_qualified:
            # use mask to use or ignore the daily traffic prediction.
            for price_cat in cfg['price_cats']:
                if mask[es_daily_ucdoc['uckey']][es_daily_ucdoc['day']][price_cat]:
                    predicted_traffic +=  es_daily_ucdoc[es_daily_ucdoc['day']][price_cat]
                    es_uckeys_used.add(es_daily_ucdoc['uckey'])
    es_uckeys_used_count = len(es_uckeys_used)

    return int(predicted_traffic), es_uckeys_used_count


def get_days(starting_day, ending_day):
    # get series of day strs. e.g. 10 continual days.
    days = []
    day = datetime.strptime(starting_day, '%Y-%m-%d')
    end_date = datetime.strptime(ending_day, '%Y-%m-%d')
    while day <= end_date:
        day_str = day.strftime('%Y-%m-%d')
        days.append(day_str)
        day = day + timedelta(days=1)
    return days


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


def generate_mask(starting_day, ending_day, dfd, df):
    # create mask based on the real fact data
    # set the mask to 1 if the uckey has non-zero traffic on that day, on default to 0.
    mask = dict()
    days = get_days(starting_day, ending_day)
    dfd_uckeys = dfd.select(dfd.uckey).distinct()

    for dfd_uckey in dfd_uckeys.collect():
        mask[dfd_uckey.uckey] = {}
        for day in days:
            mask[dfd_uckey.uckey][day] = {}
            # added the mask based on price_cat level.
            for price_cat in cfg['price_cats']:
                mask[dfd_uckey.uckey][day][price_cat] = 0

    # if the fact data's uckey meets all the conditions and has daily traffic in that predicted day.
    for df_ in df.collect():
        mask[df_.uckey][df_.day]['h'+str(df_.price_cat)] = 1

    return mask


def load_factdata_traffic(sc, cfg, starting_day, ending_day, attributes_condition):
    hive_context = HiveContext(sc)
    bucket_id_max = cfg['bucket_id_max']
    # step 1: load the original fact data.
    # step 2: load the distribution e.g. 133904 uckeys.
    # step 3: inner join the original fact data with the distribution. e.g. 133904 uckeys.
    # step 4: filter the new fact data with date range e.g. 2020-01-30 - 2020-02-08, 10 days.
    # step 5: filter the new fact data with conditions.
    # step 6: calculate the filtered fact data's total traffic.

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
    df = df.groupBy('uckey', 'day', 'price_cat').agg(
    {"count": "sum"}).withColumnRenamed("sum(count)", "daily_price_cat_count")
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
    # e.g. [Row(uckey=u'magazinelock,01,2G,,,CPM,13', day=u'2020-01-19', hour=8, price_cat=1, daily_price_cat_count=10,
    # m=u'magazinelock', si=u'01', t=u'2G', g=u'', a=u'', pm=u'CPM', r=u'13')]
    if attributes_condition:
        for attr, attr_value in attributes_condition.items():
            df = df.filter(df[attr] == str(attr_value))

    # step 6: the final filtered df is ready to get the total traffic count.

    df_traffic = df.agg({"daily_price_cat_count": "sum"}).withColumnRenamed(
        "sum(daily_price_cat_count)", "sum_traffic")
    factdata_traffic = df_traffic.take(1)[0]["sum_traffic"]
    factdata_uckeys_used_count = df.select(df.uckey).distinct().count()

    # generate the mask based on the distribution and the filtered fact data.
    mask = generate_mask(starting_day, ending_day, dfd, df)

    return factdata_traffic, factdata_uckeys_used_count, mask


def calculate_error(factdata_traffic, predicted_traffic):
    mape_error = 1.0 * abs(factdata_traffic - predicted_traffic) / \
        factdata_traffic if factdata_traffic else 0
    smape_error = 2.0 * abs(factdata_traffic - predicted_traffic) / \
        (factdata_traffic + predicted_traffic) if factdata_traffic else 0
    mape_error = "%.2f" % round(mape_error, 2)
    smape_error = "%.2f" % round(smape_error, 2)

    return mape_error, smape_error


def run_testcase(sc, cfg, starting_day, ending_day, attributes_condition, es_daily_ucdocs):
    # step 1: load the filtered factdata and calculate the factdata's traffic.
    # step 2: calculate the es predictions' traffic.
    # step 3: compare the two traffic and output the error result.
    factdata_traffic, factdata_uckeys_used_count, mask = load_factdata_traffic(
        sc, cfg, starting_day, ending_day, attributes_condition)

    predicted_traffic, es_uckeys_used_count = calculate_es_predictions_traffic(
        attributes_condition, es_daily_ucdocs, mask)

    mape_error, smape_error = calculate_error(
        factdata_traffic, predicted_traffic)

    return [starting_day, ending_day, str(attributes_condition), str(factdata_uckeys_used_count),
            str(es_uckeys_used_count), str(factdata_traffic),
            str(predicted_traffic), str(mape_error), str(smape_error)]


def transform_ucdocs(es_ucdocs, starting_day, ending_day):
    es_daily_ucdocs = []
    uckey_attrs = cfg['uckey_attrs']
    # ['m', 'si', 't', 'g', 'a', 'pm', 'r']
    # 'r' can be replaced by 'ipl' if 'ipl' is used as the region id.
    # transform the es ucdocs to daily es ucdocs by days. e.g. 10 days.
    # agg each ucdoc's total daily hours traffic as daily traffic (uckey, day, daily_count).
    for es_ucdoc in es_ucdocs:
        day = datetime.strptime(starting_day, '%Y-%m-%d')
        end_date = datetime.strptime(ending_day, '%Y-%m-%d')
        while day <= end_date:
            day_str = day.strftime('%Y-%m-%d')
            es_ucdoc_transformed = dict()
            #uckey, day, [day_str][price_cat] = daily_price_count
            es_ucdoc_transformed["uckey"] = es_ucdoc['_source']['uckey']
            for uckey_attr in uckey_attrs:
                es_ucdoc_transformed[uckey_attr] = str(es_ucdoc['_source']['ucdoc'][uckey_attr])
            es_ucdoc_transformed["day"] = day_str

            hours = es_ucdoc['_source']['ucdoc']['predictions'][day_str]['hours']
            """
            "hours" : [
                    {
                        "h2" : 0.0,
                        "h3" : 0.0,
                        "h0" : 0.0,
                        "h1" : 468.974853404,
                        "total" : 468.974853404
                    },
                  ]
            """
            # hours is a list of 24 hours with total count in each hour.
            # save the traffic of each price category to the daily ucdoc.
            es_ucdoc_transformed[day_str]  = {}
            for price_cat in cfg['price_cats']:
                daily_price_count = 0
                for hour_dict in hours:
                    daily_price_count += float(hour_dict[price_cat])
                es_ucdoc_transformed[day_str][price_cat] = daily_price_count
            es_daily_ucdocs.append(es_ucdoc_transformed)

            day = day + timedelta(days=1)

    return es_daily_ucdocs


def run(cfg):
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    # prepare the filters: date range and the attrs conditions.
    starting_day, ending_day = "2020-01-30", "2020-02-08"
    res = [['starting_day', 'ending_day', 'attributes_condition', 'factdata_uckeys_used_count', 'es_uckeys_used_count',
            'factdata_traffic', 'predicted_traffic', 'mape_error', 'smape_error']]
    attributes_conditions = [None, {"g": "g_m"}, {"g": "g_f"}, {"a": "4"}, {"a": "6"}, {"g": "g_m", "a": "4"},
                             {"g": "g_f", "a": "4"}, {"g": "g_f", "a": "6"}, {
                                 "r": "40"}, {"r": "49"},
                             {"r": "80"}, {"m": "native"}, {"pm": "CPC"},
                             {"g": "g_f", "a": "6", "m": "native"},  {
                                 "g": "g_f", "a": "6", "m": "native", "r": "40"}
                             ]

    # prepare the shared es ucdocs and the transformed daily ucdocs.
    es_ucdocs = load_es_predictions(cfg)
    es_daily_ucdocs = transform_ucdocs(es_ucdocs, starting_day, ending_day)

    try:
        for attributes_condition in attributes_conditions:
            print('processing ' + str((starting_day, ending_day)) +
                  ', ' + str(attributes_condition))
            res.append(run_testcase(sc, cfg, starting_day,
                                    ending_day, attributes_condition, es_daily_ucdocs))
    finally:
        sc.stop()

    for resi in res:
        print(", ".join(resi))


# spark-submit --master yarn --num-executors 20 --executor-cores 5 --jars lib/elasticsearch-hadoop-6.5.2.jar tests/test_dlpredictor_system_errors.py tests/conf/config.yml
if __name__ == '__main__':

    logger = logging.getLogger('test-system-errors-dlpredictor-logger')
    parser = argparse.ArgumentParser(
        description='test system errors of dlpredictor')
    parser.add_argument('config_file')
    args = parser.parse_args()

    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            print("Successfully open {}".format(args.config_file))
    except IOError as e:
        logger.error(
            "Open config file unexpected error: I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        logger.error("Unexpected error:{}".format(sys.exc_info()[0]))
        raise

    run(cfg)
