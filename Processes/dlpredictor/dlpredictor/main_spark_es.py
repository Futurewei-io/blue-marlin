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

# -*- coding: UTF-8 -*-
import os
import random
import sys
import ast
import json
import time
import yaml
import argparse

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, create_map, sum as sum_agg, struct
from pyspark.sql.types import IntegerType, StringType, ArrayType, MapType, FloatType, StructField, StructType
from pyspark.sql import HiveContext
from imscommon.model.ucdoc import UCDoc
from imscommon.model.ucday import UCDay
from imscommon.model.uchour import UCHour
from dlpredictor.prediction.forecaster import Forecaster
from dlpredictor.prediction.ims_predictor_util import convert_records_map_to_list
from dlpredictor.log import *
from dlpredictor.util.sparkesutil import *
from dlpredictor import transform
from itertools import chain
from datetime import datetime, timedelta


# [{14: [u'1:3']}, {13: [u'1:3']}, {11: [u'1:3']}, {15: [u'1:5']}, {22: [u'1:8']}, {23: [u'1:6']}, {19: [u'1:1']}, {18: [u'1:1']}, {12: [u'1:5']}, {17: [u'1:5']}, {20: [u'1:3']}, {21: [u'1:21']}]


def sum_count_array(hour_counts):
    result_map = {}
    for item in hour_counts:
        for _, v in item.items():
            for i in v:
                key, value = i.split(':')
                if key not in result_map:
                    result_map[key] = 0
                result_map[key] += int(value)

    result = []
    for key, value in result_map.items():
        result.append(key+":"+str(value))
    return result

# ca1 = [u'1:9']


def add_count_arrays(ca1, ca2):
    result_map = {}
    for i in ca1+ca2:
        key, value = i.split(':')
        if key not in result_map:
            result_map[key] = 0
        result_map[key] += int(value)
    result = []
    for key, value in result_map.items():
        result.append(key+":"+str(value))
    return result

# [{u'2019-11-02': [u'1:9']}]


def sum_day_count_array(day_count_arrays):
    result_map = {}
    for day_count_array in day_count_arrays:
        for item in day_count_array:
            for day, v in item.items():
                if not day:
                    continue
                if day not in result_map:
                    result_map[day] = []
                result_map[day] = add_count_arrays(result_map[day], v)

    return [result_map]


def __save_as_table(df, table_name, hive_context, create_table):

    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

        df.createOrReplaceTempView("r907_temp_table")

        command = """
            CREATE TABLE IF NOT EXISTS {} as select * from r907_temp_table
            """.format(table_name)

        hive_context.sql(command)


def run(cfg, yesterday, model_name, model_version, serving_url):

    # os.environ[
    #     'PYSPARK_SUBMIT_ARGS'] = '--jars /home/reza/eshadoop/elasticsearch-hadoop-6.5.2/dist/elasticsearch-hadoop-6.5.2.jar pyspark-shell'

    es_write_conf = {"es.nodes": cfg['es_host'],
                     "es.port": cfg['es_port'],
                     "es.resource": cfg['es_predictions_index']+'/'+cfg['es_predictions_type'],
                     "es.batch.size.bytes": "1000000",
                     "es.batch.size.entries": "100",
                     "es.input.json": "yes",
                     "es.mapping.id": "uckey",
                     "es.nodes.wan.only": "true",
                     "es.write.operation": "upsert"}

    sc = SparkContext()
    hive_context = HiveContext(sc)
    forecaster = Forecaster(cfg)
    sc.setLogLevel(cfg['log_level'])

    # Reading the max bucket_id
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata = cfg['factdata']
    distribution_table = cfg['distribution_table']
    norm_table = cfg['norm_table']
    traffic_dist = cfg['traffic_dist']

    model_stats = get_model_stats(cfg, model_name, model_version)

    # Read dist
    command = """
        SELECT
        DIST.uckey,
        DIST.ratio,
        DIST.cluster_uckey,
        DIST.price_cat
        FROM {} AS DIST
        """.format(distribution_table)
    df_dist = hive_context.sql(command)

    # Read norm table
    # DataFrame[uckey: string, ts: array<int>, p: float, a__n: float, a_1_n: float, a_2_n: float, a_3_n: float, a_4_n: float, a_5_n: float, a_6_n: float, t_UNKNOWN_n: float, t_3G_n: float, t_4G_n: float, t_WIFI_n: float, t_2G_n: float, g__n: float, g_g_f_n: float, g_g_m_n: float, g_g_x_n: float, price_cat_1_n: float, price_cat_2_n: float, price_cat_3_n: float, si_vec_n: array<float>, r_vec_n: array<float>, p_n: float, ts_n: array<float>]
    command = """
        SELECT
        uckey AS cluster_uckey,
        price_cat,
        a__n,a_1_n,a_2_n,a_3_n,a_4_n,a_5_n,a_6_n,
        t_UNKNOWN_n,t_3G_n,t_4G_n,t_WIFI_n,t_2G_n,
        g__n, g_g_f_n, g_g_m_n, g_g_x_n,
        price_cat_1_n, price_cat_2_n, price_cat_3_n,
        si_vec_n,
        r_vec_n
        FROM {}
        """.format(norm_table)
    df_norm = hive_context.sql(command)
    # df_norm = df_norm.groupBy('cluster_uckey', 'a__n', 'a_1_n', 'a_2_n', 'a_3_n', 'a_4_n', 'a_5_n', 'a_6_n', 't_UNKNOWN_n',
    #                           't_3G_n', 't_4G_n', 't_WIFI_n', 't_2G_n', 'g__n', 'g_g_f_n', 'g_g_m_n', 'g_g_x_n', 'si_vec_n').count().drop('count')


    # create day_list from yesterday for train_window
    duration = model_stats['model']['duration']
    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, duration):
        day_list.append(datetime.strftime(day, '%Y-%m-%d'))
        day = day + timedelta(days=-1)
    day_list.sort()

    df_prediction_ready = None
    df_uckey_cluster = None
    start_bucket = 0

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
        """.format(factdata, str(start_bucket), str(end_bucket))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)

        # [Row(count_array=[u'1:504'], day=u'2019-11-02', hour=2, uckey=u'magazinelock,04,WIFI,g_m,1,CPM,78', hour_price_imp_map={2: [u'1:504']})]
        df = df.withColumn('hour_price_imp_map',
                           expr("map(hour, count_array)"))

        # [Row(uckey=u'native,68bcd2720e5011e79bc8fa163e05184e,4G,g_m,2,CPM,19', day=u'2019-11-02', hour_price_imp_map_list=[{15: [u'3:3']}, {7: [u'3:5']}, {10: [u'3:3']}, {9: [u'3:1']}, {16: [u'3:2']}, {22: [u'3:11']}, {23: [u'3:3']}, {18: [u'3:7']}, {0: [u'3:4']}, {1: [u'3:2']}, {19: [u'3:10']}, {8: [u'3:4']}, {21: [u'3:2']}, {6: [u'3:1']}])]
        df = df.groupBy('uckey', 'day').agg(
            collect_list('hour_price_imp_map').alias('hour_price_imp_map_list'))

        # [Row(uckey=u'native,68bcd2720e5011e79bc8fa163e05184e,4G,g_m,2,CPM,19', day=u'2019-11-02', day_price_imp=[u'3:58'])]
        df = df.withColumn('day_price_imp', udf(
            sum_count_array, ArrayType(StringType()))(df.hour_price_imp_map_list)).drop('hour_price_imp_map_list')

        # [Row(uckey=u'native,68bcd2720e5011e79bc8fa163e05184e,4G,g_m,2,CPM,19', day=u'2019-11-02', day_price_imp=[u'3:58'], day_price_imp_map={u'2019-11-02': [u'3:58']})]
        df = df.withColumn('day_price_imp_map', expr(
            "map(day, day_price_imp)"))

        # [Row(uckey=u'native,z041bf6g4s,WIFI,g_f,1,CPM,71', day_price_imp_map_list=[{u'2019-11-02': [u'1:2', u'2:261']}, {u'2019-11-03': [u'2:515']}])])
        df = df.groupBy('uckey').agg(collect_list(
            'day_price_imp_map').alias('day_price_imp_map_list'))

        # [Row(uckey=u'native,z041bf6g4s,WIFI,g_f,1,CPM,71', day_price_imp_map_list=[{u'2019-11-02': [u'1:2', u'2:261']}, {u'2019-11-03': [u'2:515']}], ratio=0.09467455744743347, cluster_uckey=u'892', price_cat=u'1')]
        df = df.join(df_dist, on=['uckey'], how='inner')

        # df_uckey_cluster keeps the ratio and cluster_key for only uckeys that are being processed
        if not df_uckey_cluster:
            df_uckey_cluster = df.select(
                'uckey', 'cluster_uckey', 'ratio', 'price_cat')
            df_uckey_cluster.cache()
        else:
            df_uckey_cluster = df.select(
                'uckey', 'cluster_uckey', 'ratio', 'price_cat').union(df_uckey_cluster)
            df_uckey_cluster.cache()

        # [Row(cluster_uckey=u'2469', price_cat=u'2', cluster_day_price_imp_list=[[{u'2019-11-02': [u'2:90']}, {u'2019-11-03': [u'2:172']}]])])
        df = df.groupBy('cluster_uckey', 'price_cat').agg(
            collect_list('day_price_imp_map_list').alias('cluster_day_price_imp_list'))

        df = df.withColumn('ts', udf(sum_day_count_array,
                                     ArrayType(MapType(StringType(), ArrayType(StringType()))))(df.cluster_day_price_imp_list))

        # [Row(cluster_uckey=u'2469', price_cat=u'2', ts=[{u'2019-11-02': [u'2:90'], u'2019-11-03': [u'2:172']}])]
        df = df.drop('cluster_day_price_imp_list')

        if not df_prediction_ready:
            df_prediction_ready = df
            df_prediction_ready.cache()
        else:
            df = df_prediction_ready.union(df)
            df = df.groupBy('cluster_uckey', 'price_cat').agg(
                collect_list('ts').alias('ts_list'))
            df = df.withColumn('ts', udf(sum_day_count_array,
                                         ArrayType(MapType(StringType(), ArrayType(StringType()))))(df.ts_list))
            df = df.drop('ts_list')

            # [Row(cluster_uckey=u'magazinelock,03,WIFI,g_f,1,CPM,60', ts=[{u'2019-11-02': [u'1:2']}])]
            df_prediction_ready = df
            df_prediction_ready.cache()

    # [Row(cluster_uckey=u'1119', price_cat=u'2', ts=[{u'2019-11-02': [u'1:862', u'3:49', u'2:1154'], u'2019-11-03': [u'1:596', u'3:67', u'2:1024']}])]
    df = df_prediction_ready

    df = df.join(df_norm, on=['cluster_uckey', 'price_cat'], how='inner')

    # [Row(cluster_uckey=u'1119', price_cat=u'2', ts=[{u'2019-11-02': [u'1:862', u'3:49', u'2:1154'], u'2019-11-03': [u'1:596', u'3:67', u'2:1024']}], a__n=-0.005224577616900206, a_1_n=0.6089736819267273, a_2_n=-0.21013110876083374, a_3_n=0.16884993016719818, a_4_n=-0.3416250944137573, a_5_n=0.15184317529201508, a_6_n=-0.16529197990894318, t_UNKNOWN_n=-0.4828081429004669, t_3G_n=1.2522615194320679, t_4G_n=-0.15080969035625458, t_WIFI_n=-0.35078370571136475, t_2G_n=1.991615653038025, g__n=-0.08197031915187836, g_g_f_n=0.010901159606873989, g_g_m_n=-0.21557298302650452, g_g_x_n=1.4449801445007324, price_cat_1_n=-1.2043436765670776, price_cat_2_n=1.885549783706665, price_cat_3_n=-0.48205748200416565, si_vec_n=[-0.20294927060604095, -0.27017056941986084, -0.16821187734603882, -0.20294314622879028, -0.11777336895465851, 0.9738097786903381, 0.23326143622398376, -0.16500996053218842, -0.19148004055023193, -0.15753313899040222, -0.149298757314682, -0.19954630732536316, -0.15968738496303558, 0.12466698884963989, -0.15369804203510284, 0.04789407551288605, -0.22501590847969055, 0.14411255717277527, -0.209896981716156, -0.17969290912151337, 0.06794296950101852, -0.12367484718561172, 0.5581679344177246, 0.8108972311019897, -0.20487570762634277, 2.597964286804199, -0.2720063328742981, 0.1152268648147583, 0.27174681425094604, -0.20653237402439117, -0.2899857461452484, -0.15441325306892395, -0.17766059935092926, -0.11622612923383713, 0.3738412857055664, 1.0858312845230103, 0.6114567518234253], r_vec_n=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], uckey=u'native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPC,5', ratio=0.11989551782608032)]
    df = df.join(df_uckey_cluster, on=[
                 'cluster_uckey', 'price_cat'], how='inner')

    predictor_udf = udf(transform.predict_daily_uckey(days=day_list,
        serving_url=serving_url, forecaster=forecaster, model_stats=model_stats, columns=df.columns), MapType(StringType(), FloatType()))

    df = df.withColumn('day_prediction_map',
                       predictor_udf(struct([df[name] for name in df.columns])))

    # [Row(cluster_uckey=u'1119', price_cat=u'2', day_prediction_map={u'2019-11-02': 220.0, u'2019-11-03': 305.0}, ratio=0.11989551782608032, uckey=u'native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPC,5')]
    df = df.select('cluster_uckey', 'price_cat',
                   'day_prediction_map', 'ratio', 'uckey')

    # [Row(ucdoc_elements=Row(price_cat=u'2', ratio=0.11989551782608032, day_prediction_map={u'2019-11-02': 220.0, u'2019-11-03': 305.0}), uckey=u'native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPC,5')]
    ucdoc_elements_type = StructType([StructField('price_cat', StringType(), False), StructField(
        'ratio', FloatType(), False), StructField('day_prediction_map', MapType(StringType(), FloatType()), False)])
    df = df.withColumn('ucdoc_elements_pre_price_cat', udf(lambda price_cat, ratio, day_prediction_map:
                                                           (price_cat, ratio, day_prediction_map), ucdoc_elements_type)(df.price_cat, df.ratio, df.day_prediction_map)).select('ucdoc_elements_pre_price_cat', 'uckey')

    # [Row(uckey=u'splash,d971z9825e,WIFI,g_m,1,CPT,74', ucdoc_elements=[Row(price_cat=u'1', ratio=0.5007790923118591, day_prediction_map={u'2019-11-02': 220.0, u'2019-11-03': 305.0})])]
    df = df.groupBy('uckey').agg(collect_list('ucdoc_elements_pre_price_cat').alias('ucdoc_elements'))

    df = df.withColumn('prediction_output', udf(transform.generate_ucdoc(traffic_dist), StringType())(
        df.uckey, df.ucdoc_elements))

    df_predictions_doc = df.select('uckey', 'prediction_output')
    rdd = df_predictions_doc.rdd.map(lambda x: transform.format_data(x, 'ucdoc'))
    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)

    sc.stop()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    parser.add_argument('yesterday')
    parser.add_argument('model_name')
    parser.add_argument('model_version')
    parser.add_argument('serving_url')
    args = parser.parse_args()

    # Load config file
    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            logger_operation.info(
                "Successfully open {}".format(args.config_file))
    except IOError as e:
        logger_operation.error(
            "Open config file unexpected error: I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        logger_operation.error("Unexpected error:{}".format(sys.exc_info()[0]))
        raise

    run(cfg, args.yesterday, args.model_name, args.model_version, args.serving_url)
