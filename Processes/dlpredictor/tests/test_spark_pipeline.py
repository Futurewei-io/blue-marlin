# -*- coding: UTF-8 -*-
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType
from imscommon.model.ucdoc import UCDoc
from imscommon.model.ucday import UCDay
from imscommon.model.uchour import UCHour
from imscommon.es.ims_esclient import ESClient
from dlpredictor.prediction.forecaster import Forecaster
from dlpredictor.prediction.ims_predictor_util import convert_records_map_to_list
from dlpredictor.log import *
from dlpredictor.util.sparkesutil import *
from dlpredictor.transform import *
from predictor_dl_model.trainer.feeder import VarFeeder

import os
import random
import sys
import ast
import json
import time
import yaml
import argparse
import pickle

from predictor_dl_model.trainer import feeder
sys.modules['feeder'] = feeder


def run(cfg, model_name, model_verion):

    sc = SparkContext()
    hive_context = HiveContext(sc)
    forecaster = Forecaster(cfg)
    sc.setLogLevel('WARN')

    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata = cfg['factdata']

    model_stats = get_model_stats(cfg,model_name,model_verion)
    print(model_stats)

    # feeder = None
    # with open("data/vars/feeder_meta.pkl", mode='rb') as file:
    #     feeder = pickle.load(file)
    # print(type(feeder))
    # print(feeder)

    # # inp = VarFeeder.read_vars("data/vars")
    # # print(type(inp))
    # # Writing a JSON file
    # # with open('data.json', 'w') as f:
    # #     json.dump(inp, f)

    # feeder_brc = sc.broadcast(feeder)

    start_bucket = 0
    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = """
        select count_array,day,hour,uckey from {} where day between '2018-01-01' and '2018-01-05' and bucket_id between {} and {}
        """.format(factdata, str(start_bucket), str(end_bucket))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)

        df = df.withColumn('hour_count', expr("map(hour, count_array)"))

        df = df.groupBy('uckey', 'day').agg(
            collect_list('hour_count').alias('hour_counts'))

        df = df.withColumn('day_hour_counts', expr("map(day, hour_counts)"))

        df = df.groupBy('uckey').agg(collect_list(
            'day_hour_counts').alias('prediction_input'))

        l = df.take(1)
        row = l[0]
        predict_counts_for_uckey(forecaster, model_stats, cfg)(row['uckey'], row['prediction_input'])
        break

        predictor_udf = udf(predict_counts_for_uckey(forecaster, model_stats, cfg), StringType())

        df = df.withColumn('prediction_output', predictor_udf(df.uckey, df.prediction_input))

        l = df.take(1)
        row = l[0]
        print(row['prediction_output'])
        break

        # predictor_hrdist_udf = udf(
        #     predict_hourly_counts_for_uckey(forecaster, cfg), StringType())

        # df = df.withColumn('hrdist_output', predictor_hrdist_udf(
        #     df.uckey, df.prediction_input, df.prediction_output))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
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

    run(cfg, 's32', '1')
