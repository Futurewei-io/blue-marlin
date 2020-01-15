# Copyright 2019, Futurewei Technologies
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
#                                                 * "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# This is a spark program that converts factdata hive table into tfrecords and saves it on hdfs.
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, FloatType
from datetime import datetime, timedelta
import math
import pickle
import statistics

import predictor_dl_model.pipeline.transform as transform


def __save_as_table(df, table_name, hive_context, create_table):

    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

        command = """
            CREATE TABLE IF NOT EXISTS {}
            (
            uckey string,
            price_cat string,
            hour int,
            uph string,
            ts array<float>,
            ts_n array<float>,
            a string,
            g string,
            t string,
            si string,
            r string,
            price_cat_0_n float, price_cat_1_n float, price_cat_2_n float, price_cat_3_n float,
            g_g_m int, g_g_f int, g_g_x int,
            g_g_m_n float, g_g_f_n float, g_g_x_n float,
            a_1_n float, a_2_n float, a_3_n float, a_4_n float,
            t_3G_n float,t_4G_n float,
            si_1_n float,si_2_n float,si_3_n float
            )
            """.format(table_name)

        hive_context.sql(command)

    df.select('uckey',
              'price_cat',
              'hour',
              'uph',
              'ts',
              'ts_n',
              'a',
              'g',
              't',
              'si',
              'r',
              'price_cat_0_n', 'price_cat_1_n', 'price_cat_2_n', 'price_cat_3_n',
              'g_g_m', 'g_g_f', 'g_g_x',
              'g_g_m_n', 'g_g_f_n', 'g_g_x_n',
              'a_1_n', 'a_2_n', 'a_3_n', 'a_4_n',
              't_3G_n', 't_4G_n',
              'si_1_n', 'si_2_n', 'si_3_n'
              ).write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)

def normalize(mlist):
    avg = statistics.mean(mlist)
    std = statistics.stdev(mlist)
    return [(item-avg)/(std) for item in mlist]

def prepare_tfrecords(hive_context, factdata_table_name, yesterday, past_days, table_name, bucket_size, bucket_step, tf_statistics_path, holidays):

    # ts will be counts from yesterday-(past_days) to yesterday

    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, past_days):
        day_list.append(datetime.strftime(day, '%Y-%m-%d'))
        day = day + timedelta(days=-1)
    day_list.sort()

    dow_list = []
    for day in day_list:
        dday = datetime.strptime(day, '%Y-%m-%d')
        dow_list.append(float(dday.weekday()))

    tsf = {}
    week_period = 7.0 / (2 * math.pi)
    sin_list = [math.sin(x / week_period) for x in dow_list]
    cos_list = [math.cos(x / week_period) for x in dow_list]
    tsf['dow_sin'] = sin_list
    tsf['dow_cos'] = cos_list

    tsf['days'] =   day_list
    holidays_norm = normalize([1 if day in holidays else 0 for day in day_list])

    output = open(tf_statistics_path, 'wb')
    pickle.dump(tsf, output)
    output.close()

    start_bucket = 0
    first_round = True
    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = """
        select count_array,day,hour,uckey from {} where bucket_id between {} and {}
        """.format(factdata_table_name, str(start_bucket), str(end_bucket))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)
        # [Row(count_array=[u'0:0', u'1:0', u'2:0', u'3:0'], day=u'2018-03-09', hour=0, uckey=u'banner,1,3G,g_f,1,pt,1002,icc')]

        df = transform.add_count_map(df)

        # Explode count_map to have pcat and count on separate columns
        df = df.select('uckey', 'day', 'hour', explode(df.count_map)).withColumnRenamed(
            "key", "price_cat").withColumnRenamed("value", "count")

        df = df.withColumn('day_count', expr("map(day, count)"))

        df = df.groupBy('uckey', 'hour', 'price_cat').agg(
            collect_list('day_count').alias('ts_list_map'))

        # This method handles missing dates by injecting nan
        df = transform.calculate_time_series(df, day_list)

        # remove rows with less than 50 percent nan or 0
        df = transform.clean_data(df, 0.3 * past_days)

         # replace nan with median
        df = transform.replace_with_median(df)

        df = transform.add_uph(df)

        # Log processor code to know the index of features
        # v = concat_ws(UCDoc.uckey_delimiter, df.adv_type 0 , df.slot_id 1 , df.net_type 2 , df.gender 3 , df.age 4 ,
        #                   df.price_dev 5 , df.pricing_type 6 , df.residence_city 7 , df.ip_city_code 8 )
        df = df.withColumn('a', transform.add_feature_udf(4)(df.uckey))
        df = df.withColumn('si', transform.add_feature_udf(1)(df.uckey))
        df = df.withColumn('r', transform.add_feature_udf(7)(df.uckey))
        df = df.withColumn('t', transform.add_feature_udf(0)(df.uckey))
        df = df.withColumn('g', transform.add_feature_udf(3)(df.uckey))

        collection_map = {}
        # feature_value_list is important when calling model serving
        feature_name = 'g'
        feature_value_list = ['g_m', 'g_f', 'g_x']
        collection_map[feature_name] = feature_value_list

        feature_name = 'a'
        feature_value_list = ['1', '2', '3', '4']
        collection_map[feature_name] = feature_value_list

        feature_name = 't'
        feature_value_list = ['3G', '4G']
        collection_map[feature_name] = feature_value_list

        feature_name = 'si'
        feature_value_list = ['1', '2', '3']
        collection_map[feature_name] = feature_value_list

        # Hour OneHotEncoding Scalling
        # feature_name = 'hour'
        # feature_value_list = range(0, 24)
        # collection_map[feature_name] = feature_value_list

        feature_name = 'price_cat'
        feature_value_list = ['0', '1', '2', '3']
        collection_map[feature_name] = feature_value_list

        for feature_name, feature_value_list in collection_map.items():
            df = transform.add_ohe_feature(df, feature_name, feature_value_list)
            for feature_value in feature_value_list:
                ohe_feature = feature_name + '_' + str(feature_value)
                df = transform.normalize_ohe_feature(
                    df, ohe_feature=ohe_feature)

        __save_as_table(df, table_name, hive_context, first_round)
        first_round = False


if __name__ == "__main__":
    sc = SparkContext()
    hive_context = HiveContext(sc)

    yesterday = "2018-02-01"
    past_days = 31
    table_name = 'trainready_t1'
    bucket_size = 64
    bucket_step = 10
    tf_statistics_path = 'tf_statistics.pkl'
    holidays = []
    
    prepare_tfrecords(hive_context, 'factdata', yesterday,
                      past_days, table_name, bucket_size, bucket_step, tf_statistics_path, holidays)
