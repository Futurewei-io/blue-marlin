# -*- coding: UTF-8 -*-
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType
from imscommon.model.ucdoc import UCDoc
from imscommon.model.ucday import UCDay
from imscommon.model.uchour import UCHour
from dlpredictor.prediction.forecaster import Forecaster
from dlpredictor.prediction.ims_predictor_util import convert_records_map_to_list
from dlpredictor.log import *
from dlpredictor.util.sparkesutil import *
from dlpredictor.transform import *

import os
import random
import sys
import ast
import json
import time
import yaml
import argparse


def run(cfg, model_name, model_version, serving_url):

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
    sc.setLogLevel('WARN')

    # Reading the max bucket_id
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata = cfg['factdata']

    model_stats = get_model_stats(cfg, model_name, model_version)

    start_bucket = 0
    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = """
        select count_array,day,hour,uckey from {} where bucket_id between {} and {} and not day='2018-03-29' and not day='2018-03-30' and not day='2018-03-31'
        """.format(factdata, str(start_bucket), str(end_bucket))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)

        df = df.withColumn('hour_count', expr("map(hour, count_array)"))

        df = df.groupBy('uckey', 'day').agg(
            collect_list('hour_count').alias('hour_counts'))

        df = df.withColumn('day_hour_counts', expr("map(day, hour_counts)"))

        df = df.groupBy('uckey').agg(collect_list(
            'day_hour_counts').alias('prediction_input'))

        predictor_udf = udf(predict_counts_for_uckey(serving_url, forecaster, model_stats, cfg), StringType())

        df = df.withColumn('prediction_output', predictor_udf(
            df.uckey, df.prediction_input))

        df_predictions_doc = df.select('uckey', 'prediction_output')
        rdd = df_predictions_doc.rdd.map(lambda x: format_data(x, 'ucdoc'))
        rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
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

    run(cfg, args.model_name, args.model_version, args.serving_url)
