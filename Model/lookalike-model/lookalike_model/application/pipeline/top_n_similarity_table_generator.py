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
import pyspark.sql.functions as fn

from pyspark import SparkContext
from pyspark.sql import HiveContext, SparkSession
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, MapType, IntegerType
from pyspark.sql.functions import udf, col, explode

from math import sqrt
import time
import numpy as np
import itertools
import heapq
from util import resolve_placeholder
from lookalike_model.pipeline.util import write_to_table, write_to_table_with_partition

'''
This process generates the top-n-similarity table.

spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict top_n_similarity_table_generator.py config.yml

The top-n-similarity table is 

|user| top-N-similarity|top-n-users
|:-------------| :------------: |
|user-1-did| [similarity-score-11, similarity-score-12, similarity-score-13] |[user-did-1, user-did-2, user-did-3]|
|user-2-did| [similarity-score-21, similarity-score-22, similarity-score-23] |[user-did-10, user-did-20, user-did-30]|
|user-3-did| [similarity-score-31, similarity-score-32, similarity-score-33] |[user-did-23, user-did-87, user-did-45]|

'''


def run(spark_session, hive_context, cfg):

    score_matrix_table = cfg['score_matrix_table']['score_matrix_table']
    similarity_table = cfg['top_n_similarity']['similarity_table']
    did_bucket_size = cfg['top_n_similarity']['did_bucket_size']
    did_bucket_step = cfg['top_n_similarity']['did_bucket_step']
    cross_bucket_size = cfg['top_n_similarity']['cross_bucket_size']
    top_n_value = cfg['top_n_similarity']['top_n']

    first_round = True
    num_batches = (did_bucket_size + did_bucket_step - 1) / did_bucket_step
    batch_num = 1
    for did_bucket in range(0, did_bucket_size, did_bucket_step):
        print('Processing batch {} of {}   bucket number: {}'.format(batch_num, num_batches, did_bucket))

        command = "SELECT did_list, did_bucket, score_matrix, c1_list FROM {} WHERE did_bucket BETWEEN {} AND {}".format(
            score_matrix_table, did_bucket, min(did_bucket + did_bucket_step - 1, did_bucket_size))
        # |0004f3b4731abafa9ac54d04cb88782ed61d30531262decd799d91beb6d6246a|0         |
        # [0.24231663, 0.20828941, 0.0]|
        df = hive_context.sql(command)
        df = df.withColumn('top_n_similar_user', fn.array())

        for cross_bucket in range(0, cross_bucket_size):
            print('Processing batch {}, alpha bucket {}'.format(batch_num, cross_bucket))

            command = """SELECT did_list, score_matrix, c1_list, did_bucket
            FROM {} WHERE did_bucket = {} """
            command = command.format(score_matrix_table, cross_bucket)

            df_user = hive_context.sql(command)
            cross_users = df_user.select('did_list', 'score_matrix', 'c1_list').collect()

            if len(cross_users) == 0:
                continue

            cross_users_did_score = (cross_users[0]['did_list'], cross_users[0]['score_matrix'])
            c2 = np.array(cross_users[0]['c1_list'])

            def calculate_similarity(cross_users_did_score, c2):
                def __helper(user_score_matrix, top_n_user_score, c1_list):
                    user_score_matrix = np.array(user_score_matrix)
                    m = user_score_matrix.shape[1]
                    cross_dids, cross_score_matrix = cross_users_did_score
                    cross_score_matrix = np.array(cross_score_matrix)
                    cross_mat = np.matmul(user_score_matrix, cross_score_matrix.transpose())

                    similarity = np.sqrt(m) - np.sqrt(np.maximum(np.expand_dims(c1_list, 1) + c2 - (2 * cross_mat), 0.0))
                    result = []
                    for cosimilarity, top_n in itertools.izip_longest(similarity, top_n_user_score, fillvalue=[]):
                        user_score_s = list(itertools.izip(cross_dids, cosimilarity.tolist()))
                        user_score_s.extend(top_n)
                        user_score_s = heapq.nlargest(top_n_value, user_score_s, key=lambda x: x[1])
                        result.append(user_score_s)
                    return result
                return __helper

            elements_type = StructType([StructField('did', StringType(), False), StructField('score', FloatType(), False)])

            # update top_n_similar_user field
            df = df.withColumn('top_n_similar_user', udf(calculate_similarity(cross_users_did_score, c2),
                                                         ArrayType(ArrayType(elements_type)))(df.score_matrix, df.top_n_similar_user, df.c1_list))

        # Unpack the matrices into individual users.
        # Note: in Spark 2.4, the udf can be replaced with arrays_zip().
        def combine(x, y):
            return list(zip(x, y))
        df = df.withColumn("new", udf(combine, ArrayType(StructType([StructField("did", StringType()),
                                                                     StructField("top_n_similar_user", ArrayType(StructType([
                                                                         StructField("did", StringType(), True),
                                                                         StructField("score", FloatType(), True), ]), True)),
                                                                     ])))(df.did_list, df.top_n_similar_user))
        df = df.withColumn("new", explode("new"))
        df = df.select(col("new.did").alias("did"),
                       col("new.top_n_similar_user").alias("top_n_similar_user"),
                       "did_bucket")

        mode = 'overwrite' if first_round else 'append'
        # use the partitioned field at the end of the select. Order matters.
        write_to_table_with_partition(df, similarity_table, partition=('did_bucket'), mode=mode)
        first_round = False
        batch_num += 1


if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description=" ")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)
        resolve_placeholder(cfg)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('INFO')
    hive_context = HiveContext(sc)
    spark_session = SparkSession(sc)

    run(spark_session, hive_context, cfg)
    sc.stop()
    end = time.time()
    print('Runtime of the program is:', (end - start))
