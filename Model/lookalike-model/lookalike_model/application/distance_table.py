# Copyright 2020, Futurewei Technologies
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


def distance(l1):
    def _distance(l2):
        dist = sum([l1[el]*l2[el] for el, value in l1.items()])
        return dist
    return _distance


def x(l1):
    _udf_distance = udf(distance(l1), FloatType())
    return _udf_distance


def run(hive_context, cfg):
    # load dataframes
    lookalike_loaded_table_norm = cfg['output']['gucdocs_loaded_table_norm']
    keywords_table = cfg["input"]["keywords_table"]
    seeduser_table = cfg["input"]["seeduser_table"]
    lookalike_score_table = cfg["output"]["score_table"]

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(lookalike_loaded_table_norm))
    df_keywords = hive_context.sql(command.format(keywords_table))
    df_seed_user = hive_context.sql(command.format(seeduser_table))

    # creating a tuple of did and kws for seed users
    df_seed_user = df_seed_user.join(df.select('did', 'kws_norm'), on=['did'], how='left')
    # df_seed_user = df_seed_user.withColumn("seed_user_list", zip_("did", "kws"))
    seed_user_list = df_seed_user.select('did', 'kws_norm').collect()
    # seed_user list = [(did1, {k1:0, k2:0.2, ...}), (did2, )]
    # user =
    c = 0
    temp_list = []
    for item in seed_user_list:

        c += 1
        if c > 850:
            break
        df = df.withColumn(item[0], x(item[1])(col('kws_norm')))

    df.write.option("header", "true").option(
        "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(lookalike_score_table)


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
