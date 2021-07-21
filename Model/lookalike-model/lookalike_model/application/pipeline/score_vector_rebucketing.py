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
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, MapType, IntegerType
# from rest_client import predict, str_to_intlist
import requests
import json
import argparse
from pyspark.sql.functions import udf
from math import sqrt
import time
import hashlib

'''

To run, execute the following in application folder.
spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict score_vector_rebucketing.py config.yml

This process generates added secondary buckects ids (alpha-did-bucket).

'''


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


def assign_new_bucket_id(df, n, new_column_name):
    def __hash_sha256(s):
        hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
        return int(hex_value, 16)
    _udf = udf(lambda x: __hash_sha256(x) % n, IntegerType())
    df = df.withColumn(new_column_name, _udf(df.did))
    return df


def run(hive_context, cfg):

    score_vector_table = cfg['score_vector']['score_vector_table']
    bucket_size = cfg['score_vector_rebucketing']['did_bucket_size']
    bucket_step = cfg['score_vector_rebucketing']['did_bucket_step']
    alpha_bucket_size = cfg['score_vector_rebucketing']['alpha_did_bucket_size']
    score_vector_alpha_table = cfg['score_vector_rebucketing']['score_vector_alpha_table']

    first_round = True
    for start_bucket in range(0, bucket_size, bucket_step):
        command = "SELECT did, did_bucket, score_vector FROM {} WHERE did_bucket BETWEEN {} AND {}".format(score_vector_table, start_bucket, start_bucket+bucket_size-1)

        df = hive_context.sql(command)
        df = assign_new_bucket_id(df, alpha_bucket_size, 'alpha_did_bucket')
        df = df.select('did', 'did_bucket', 'score_vector', 'alpha_did_bucket')
        __save_as_table(df, table_name=score_vector_alpha_table, hive_context=hive_context, create_table=first_round)
        first_round = False


if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
    end = time.time()
    print('Runtime of the program is:', (end - start))
