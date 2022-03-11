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

# -*- coding: UTF-8 -*-
import sys
import json
import time
import yaml
import argparse
import logging

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, create_map, sum as sum_agg, struct, explode
from pyspark.sql.types import IntegerType, StringType, ArrayType, MapType, FloatType, BooleanType, LongType
from pyspark.sql import HiveContext, SQLContext
from datetime import datetime, timedelta
#import secrets
#import pickle

# from imscommon_dl.es.esclient import ESClient
# from imscommon_dl.es.es_predictions_dao import ESPredictionsDAO


from logging.config import fileConfig


from dlpredictor import transform
from dlpredictor.configutil import *
from dlpredictor.log import *
from dlpredictor.prediction.forecaster import Forecaster
from dlpredictor.util.sparkesutil import *

'''
spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 32G --driver-memory 32G --py-files dist/dlpredictor-1.6.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg,lib/predictor_dl_model-1.6.0-py2.7.egg --jars lib/elasticsearch-hadoop-6.8.0.jar experiments/dl_predictor_performance_issue/main_spark_predict_es.py experiments/dl_predictor_performance_issue/config.yml '2021-07-21'
'''

def add_count_arrays(ca1, ca2):
    '''
    ca1 = [u'1:9']
    '''
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


def sum_day_count_array(day_count_arrays):
    '''
    [{u'2019-11-02': [u'1:9']}]
    '''
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


def multiply_each_value_of_map_with_ratio(day_prediction_map, ratio):
    for k, v in day_prediction_map.items():
        day_prediction_map[k] = v * ratio
    return day_prediction_map


def get_day_count_map(map_list):
    merge_map = {}
    for map in map_list:
        for k, v in map.items():
            if not merge_map.__contains__(k):
                merge_map[k] = 0.0
            merge_map[k] = merge_map[k] + v
    date_map = {}
    for date, impr in merge_map.items():
        impr = int(impr)
        count_list_map = [{"total": impr, "h1": impr, "hr": 0}]
        date_map[date] = count_list_map
    return date_map


def add_uckey_to_json(uckey, date_count_map):
    adv_type, slot, net_type, gender, age, pricing_type, residence_city_region, city_code_region = uckey.split(",")
    prediction_json = {}
    prediction_json["cc"] = ""
    prediction_json["a"] = age
    prediction_json["algo_id"] = "dl"
    prediction_json["r"] = residence_city_region
    prediction_json["t"] = net_type
    prediction_json["ipl"] = city_code_region
    prediction_json["records"] = []
    prediction_json["si"] = slot
    prediction_json["g"] = gender
    prediction_json["cpoi"] = ""
    prediction_json["m"] = adv_type
    prediction_json["rpoi"] = ""
    prediction_json["pm"] = pricing_type
    prediction_json["uckey"] = uckey
    prediction_json["predictions"] = date_count_map
    return json.dumps(prediction_json)


def push_data_es(x):
    es_predictions_bulk_buff = []
    for it in x:
        it_predictions_str = it["hits"]
        es_predictions_bulk_buff.append(it_predictions_str)
        if len(es_predictions_bulk_buff) >= cfg['bulk_buffer_size']:
            es_predictions_dao.bulk_index(ucdocs=es_predictions_bulk_buff)
            es_predictions_bulk_buff = []
            time.sleep(secrets.choice([1, 2, 3, 4]))
    if len(es_predictions_bulk_buff) > 0:
        es_predictions_dao.bulk_index(ucdocs=es_predictions_bulk_buff)


def post_index_alias(esclient, index_new_name, index_alias):
    old_index = esclient.get_alias(index_alias)
    mesg = "removing old es index: {},  new es index: {}, es imdex alias name: {}".format(old_index, index_new_name,
                                                                                          index_alias)
    logger.info(mesg)
    esclient.remove_add_alias(old_index, index_new_name, index_alias)


def get_preditction_in_hdfs_formate(df):
    df = df.select(df.uckey, explode(df.day_count_map)).withColumnRenamed("key", "day")
    impr_udf = udf(lambda x: x[0]["total"], IntegerType())
    df = df.withColumn('impr', impr_udf(df.value)).select("uckey", "day", "impr")
    return df


def run(cfg, yesterday, serving_url):
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sqlcontext = SQLContext(sc)
    forecaster = Forecaster(cfg)
    sc.setLogLevel(cfg['log_level'])

    dl_data_path = cfg['dl_predict_ready_path']
    dl_uckey_cluster_path = cfg['dl_uckey_cluster_path']
    distribution_table = cfg['distribution_table']
    norm_table = cfg['norm_table']

    # Reza
    # model_stats = get_model_stats_using_pickel(cfg)
    model_stat_table = cfg['model_stat_table']
    model_stats = get_model_stats(hive_context, model_stat_table)
    if not model_stats:
        sys.exit("dl_spark_cmd: " + "null model stats")

    # Read dist
    command = "SELECT DIST.uckey, DIST.ratio, DIST.cluster_uckey, DIST.price_cat FROM {} AS DIST ".format(
        distribution_table)

    df_dist = hive_context.sql(command)
    df_dist = df_dist.repartition("uckey")
    df_dist.cache()

    # Read norm table
    command = "SELECT uckey AS cluster_uckey, price_cat, a__n,a_1_n,a_2_n,a_3_n,a_4_n,a_5_n,a_6_n, t_UNKNOWN_n,t_3G_n,t_4G_n,t_WIFI_n,t_2G_n, g__n, g_g_f_n, g_g_m_n, g_g_x_n, price_cat_1_n, price_cat_2_n, price_cat_3_n, si_vec_n FROM {} ".format(
        norm_table)
    df_norm = hive_context.sql(command)

    # create day_list from yesterday for train_window
    duration = model_stats['model']['duration']
    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, duration):
        day_list.append(datetime.strftime(day, '%Y-%m-%d'))
        day = day + timedelta(days=-1)
    day_list.sort()

    df = sqlcontext.read.parquet(dl_data_path)
    df_uckey_cluster = sqlcontext.read.parquet(dl_uckey_cluster_path)

    # TODO: where is sum_day_count_array?
    df = df.groupBy('cluster_uckey', 'price_cat').agg(collect_list('ts').alias('ts_list'))
    df = df.withColumn('ts',
                       udf(sum_day_count_array, ArrayType(MapType(StringType(), ArrayType(StringType()))))(df.ts_list))
    df = df.drop('ts_list')

    df = df.join(df_norm, on=['cluster_uckey', 'price_cat'], how='inner')
    df = df.join(df_uckey_cluster, on=['cluster_uckey', 'price_cat'], how='inner')

    # df = df.where(df.uckey.like('%native,b6le0s4qo8,4G,g_f,5,CPC,,1156320000%'))
    predictor_udf = udf(transform.predict_daily_uckey(days=day_list,
                                                      serving_url=serving_url, forecaster=forecaster,
                                                      model_stats=model_stats, columns=df.columns),
                        MapType(StringType(), FloatType()))

    df = df.withColumn('day_prediction_map',predictor_udf(struct([df[name] for name in df.columns])))

    df = df.select('cluster_uckey', 'price_cat', 'day_prediction_map', 'ratio', 'uckey')

    mul_udf = udf(multiply_each_value_of_map_with_ratio, MapType(StringType(), FloatType()))
    df = df.withColumn('day_prediction_map', mul_udf(df.day_prediction_map, df.ratio))

    df = df.groupBy('uckey').agg(collect_list('day_prediction_map').alias('map_list'))

    count_map_udf = udf(get_day_count_map, MapType(StringType(), ArrayType(MapType(StringType(), LongType()))))
    df = df.withColumn('day_count_map', count_map_udf(df.map_list))
    df = df.select(df.uckey, df.day_count_map)

    df.cache()
    hdfs_df = df

    df = df.withColumn('hits', udf(lambda uckey, maps: add_uckey_to_json(uckey, maps), StringType())(df.uckey,df.day_count_map)).select("hits")

    hdfs_df = get_preditction_in_hdfs_formate(hdfs_df)
    hdfs_df.show()
    
    #hdfs_df.coalesce(hdfs_write_threads).write.mode('overwrite').partitionBy("day").parquet(cfg["hdfs_prefix_path"])
    hdfs_df.write.option('header', 'true').mode('overwrite').format('hive').saveAsTable(cfg["es_predictions_index"])

    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    parser.add_argument('yesterday', help='end date in yyyy-mm-dd formate')
    args = parser.parse_args()
    # Load config file
    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
            resolve_placeholder(cfg)

    except IOError as e:
        print(
            "Open config file unexpected error: I/O error({0}): {1}".format(e.errno, e.strerror))
    except Exception as e:
        print("Unexpected error:{}".format(sys.exc_info()[0]))
        raise
    finally:
        ymlfile.close()

    yesterday = args.yesterday
    # es_json_dir = cfg["es_json_dir"]
    # index_data = dict()
    # with open(es_json_dir + '/put_predictor_index_css.json', 'r') as myfile:
    #     index_data['css'] = myfile.read()

    eligble_slot_ids = cfg['eligble_slot_ids']
    yesterday = str(yesterday)
    es_prediction_index = cfg["es_predictions_index"] + "_" + yesterday
    es_prediction_type = cfg['es_predictions_type']
    # refresh_index_wait_time = cfg["refresh_index_wait_time"]
    # es_write_threads = cfg["es_write_threads"]
    # hdfs_write_threads = cfg["hdfs_write_threads"]
    serving_url = cfg["serving_url"]

    # es_cfg = dict()
    # es_cfg['es_mode'] = cfg["es_mode"]
    # es_cfg['css_url'] = cfg["css_url"]
    # es_cfg['pem_path'] = cfg['pem_path']

    # predictions_type = dict()
    # predictions_type['css'] = cfg['es_predictions_type']

    es_predictions = " value removed"
    es_predictions_dao = "value removed"
    cfg["signKey"] = 'provide value'
    cfg["sign_prefix"] = 'provide value'
    run(cfg, yesterday, serving_url)
    # mesg = "dl_spark_cmd: ", "prediction save in ES index: ", es_prediction_index, "  ,and save the one copy  in hdfs at path: ", \
    #        cfg["hdfs_prefix_path"]
