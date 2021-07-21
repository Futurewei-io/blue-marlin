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
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, MapType
# from rest_client import predict, str_to_intlist
import requests
import json
import argparse
from pyspark.sql.functions import udf
from math import sqrt
import time

'''

To run, execute the following in application folder.
spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict score_vector_table.py config.yml

This process generates the score_vector_table table.

The top-n-similarity table is 

|user| score-vector | did-bucket
|:-------------| :------------: |
|user-1-did| [similarity-score-11, similarity-score-12, similarity-score-13] | 1
|user-2-did| [similarity-score-21, similarity-score-22, similarity-score-23] | 1
|user-3-did| [similarity-score-31, similarity-score-32, similarity-score-33] | 2

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


def run(hive_context, cfg):

    keywords_table = cfg["score_vector"]["keywords_table"]
    score_norm_table = cfg['score_vector']['score_norm_table']
    score_vector_table = cfg['score_vector']['score_vector_table']
    bucket_size = cfg['score_vector']['did_bucket_size']
    bucket_step = cfg['score_vector']['did_bucket_step']

    # get kw list
    keywords = hive_context.sql("SELECT DISTINCT(keyword) FROM {}".format(keywords_table)).collect()
    keywords = [_['keyword'] for _ in keywords]
    keywords = sorted(keywords)

    # add score-vector iterativly
    first_round = True
    for start_bucket in range(0, bucket_size, bucket_step):
        command = "SELECT did, did_bucket, kws FROM {} WHERE did_bucket BETWEEN {} AND {}".format(score_norm_table, start_bucket, start_bucket+bucket_size-1)

        # |0004f3b4731abafa9ac54d04cb88782ed61d30531262decd799d91beb6d6246a|0         |
        # [social -> 0.24231663, entertainment -> 0.20828941, reading -> 0.44120282, video -> 0.34497723, travel -> 0.3453492, shopping -> 0.5347804, info -> 0.1978679]|
        df = hive_context.sql(command)
        df = df.withColumn("score_vector",
                           udf(lambda kws: [kws[keyword] if keyword in kws else 0.0 for keyword in keywords], ArrayType(FloatType()))(df.kws))

        df = df.select('did', 'did_bucket', 'score_vector')
        __save_as_table(df, table_name=score_vector_table, hive_context=hive_context, create_table=first_round)
        first_round = False


if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description=" ")
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
