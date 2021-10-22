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

import yaml
import argparse
from pyspark import SparkContext
from pyspark.sql import HiveContext, SparkSession, Window
from pyspark.sql.functions import lit, col, udf, collect_list
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, MapType, IntegerType
# from rest_client import predict, str_to_intlist
import requests
import json
import argparse
from pyspark.sql.functions import udf
from math import sqrt
import time
import hashlib
from util import resolve_placeholder


from lookalike_model.pipeline.util import write_to_table, write_to_table_with_partition

'''

To run, execute the following in application folder.
spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict score_matirx_table.py config.yml

This process consolidates bucket score vectors into matrices.

'''


def run(spark_session, hive_context, cfg):

    score_vector_table = cfg['score_vector']['score_vector_table']
    bucket_size = cfg['score_matrix_table']['did_bucket_size']
    bucket_step = cfg['score_matrix_table']['did_bucket_step']
    score_matrix_table = cfg['score_matrix_table']['score_matrix_table']

    first_round = True
    num_batches = (bucket_size + bucket_step - 1) / bucket_step
    batch_num = 1
    for did_bucket in range(0, bucket_size, bucket_step):
        print('Processing batch {} of {}   bucket number: {}'.format(batch_num, num_batches, did_bucket))

        max_bucket = min(did_bucket+bucket_step-1, bucket_size)
        command = "SELECT did, did_bucket, score_vector, c1 FROM {} WHERE did_bucket BETWEEN {} AND {}".format(score_vector_table, did_bucket, max_bucket)
        # command = "SELECT did_bucket, collect_list(struct(did, score_vector, c1)) AS item FROM {} WHERE did_bucket BETWEEN {} AND {} GROUP BY did_bucket".format(score_vector_table, did_bucket, min(did_bucket+bucket_step-1, bucket_size))

        df = hive_context.sql(command)
        df = df.groupBy('did_bucket').agg(
            collect_list('did').alias('did_list'),
            collect_list('score_vector').alias('score_matrix'),
            collect_list('c1').alias('c1_list'))

        mode = 'overwrite' if first_round else 'append'
        write_to_table_with_partition(df.select('did_list', 'score_matrix', 'c1_list', 'did_bucket'),
                                      score_matrix_table, partition=('did_bucket'), mode=mode)
        first_round = False
        batch_num += 1


if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)
        resolve_placeholder(cfg)
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)
    spark_session = SparkSession(sc)

    run(spark_session, hive_context=hive_context, cfg=cfg)
    sc.stop()
    end = time.time()
    print('Runtime of the program is:', (end - start))
