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

import time
import datetime
from util import write_to_table_with_partition
import numpy as np
import faiss

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, FloatType, IntegerType, ArrayType, MapType

from util import resolve_placeholder, write_to_table_with_partition

'''
This process generates the top-n-similarity table.

spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict top_n_similarity.py config.yml
spark-submit --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict top_n_similarity.py config.yml

The top-n-similarity table is 

|user| top-N-similarity|top-n-users
|:-------------| :------------: |
|user-1-aid| [similarity-score-11, similarity-score-12, similarity-score-13] |[user-aid-1, user-aid-2, user-aid-3]|
|user-2-aid| [similarity-score-21, similarity-score-22, similarity-score-23] |[user-aid-10, user-aid-20, user-aid-30]|
|user-3-aid| [similarity-score-31, similarity-score-32, similarity-score-33] |[user-aid-23, user-aid-87, user-aid-45]|

'''


def load_score_vectors(spark_session, score_vector_table,
                       bucket, bucket_step, bucket_size):
    # Run the query of the Hive data.
    command = "SELECT aid, aid_bucket, score_vector, alpha_aid_bucket FROM {} WHERE alpha_aid_bucket BETWEEN {} AND {}".format(
        score_vector_table, bucket, min(bucket+bucket_step-1, bucket_size))
    df = spark_session.sql(command)

    # Get the aids, score vectors, and buckets as numpy arrays.

    # We need to collect them first and then seperate them, otherwise we cannot relate aids to scores.
    _all = df.select('aid', 'score_vector', 'aid_bucket').collect()
    aids = np.array([i['aid'] for i in _all])
    score_vectors = np.array([i['score_vector'] for i in _all], dtype='f4')
    buckets = np.array([i['aid_bucket'] for i in _all])

    # Return the aids and score_vectors.
    return (aids, score_vectors, buckets)


def run(spark_session, cfg):
    score_vector_table = cfg['score_vector_rebucketing']['score_vector_alpha_table']
    similarity_table = cfg['top_n_similarity']['similarity_table']
    top_n_value = cfg['top_n_similarity']['top_n']
    aid_bucket_size = cfg['top_n_similarity']['aid_bucket_size']
    load_bucket_step = cfg['top_n_similarity']['load_bucket_step']
    search_bucket_step = cfg['top_n_similarity']['search_bucket_step']
    index_factory_string = cfg['top_n_similarity']['index_factory_string']

    # If the number of GPUs is 0, uninstall faiss-cpu.
    num_gpus = faiss.get_num_gpus()
    assert num_gpus != 0
    print('Number of GPUs available: {}'.format(num_gpus))

    start_time = time.time()

    # Load the score vectors into the index.
    aid_list = []
    for aid_bucket in range(0, aid_bucket_size, load_bucket_step):
        print('Loading alpha buckets {} - {} of {}'.format(aid_bucket, aid_bucket+load_bucket_step-1, aid_bucket_size))
        (aids, score_vectors, _) = load_score_vectors(spark_session, score_vector_table,
                                                      aid_bucket, load_bucket_step, aid_bucket_size)

        # Keep track of the aids.
        if aid_bucket == 0:
            aid_list = aids
        else:
            aid_list = np.concatenate((aid_list, aids))

        # Create the FAISS index on the first iteration.
        if aid_bucket == 0:
            cpu_index = faiss.index_factory(score_vectors.shape[1], index_factory_string)
            gpu_index = faiss.index_cpu_to_all_gpus(cpu_index)

        # we need to check if train is necessary, now it is disabled.
        if not gpu_index.is_trained:
            gpu_index.train(score_vectors)

        # Add the vectors to the index.
        gpu_index.add(score_vectors)

    load_time = time.time()

    # Find the top N by bucket step.
    start_load = time.time()
    mode = 'overwrite'
    total_search_time = 0
    total_load_time = 0
    total_format_time = 0
    total_write_time = 0
    for aid_bucket in range(0, aid_bucket_size, search_bucket_step):
        print('Searching alpha buckets {} - {} of {}'.format(aid_bucket, aid_bucket+search_bucket_step-1, aid_bucket_size))

        # Load the users to perform the search with.
        print('Loading users from Hive')
        (aids, score_vectors, buckets) = load_score_vectors(spark_session, score_vector_table, aid_bucket, search_bucket_step, aid_bucket_size)
        end_load = time.time()
        total_load_time += end_load-start_load

        # Search for the top N similar users for bucket.
        print('Performing the search')
        top_n_distances, top_n_indices = gpu_index.search(score_vectors, top_n_value)
        end_search = time.time()
        total_search_time += end_search-end_load

        # Get the top N aids from the top N indexes.
        top_n_aids = aid_list[top_n_indices]

        # Format and write the result back to Hive.
        # Format the data for a Spark dataframe in order to write to Hive.
        #  [ ('0000001', [{'aid':'0000001', 'score':1.73205081}, {'aid':'0000003', 'score':1.73205081}, {'aid':'0000004', 'score':0.88532267}, {'aid':'0000002', 'score':0.66903623}], 0),
        #    ('0000002', [{'aid':'0000002', 'score':1.73205081}, {'aid':'0000004', 'score':1.50844401}, {'aid':'0000001', 'score':0.66903623}, {'aid':'0000003', 'score':0.66903623}], 0),
        #    ... ]
        print('Formatting the output')
        data = [(str(aid), [(str(n_aid), float(distance)) for n_aid, distance in zip(top_aid, top_distances)], int(bucket))
                for aid, top_aid, top_distances, bucket in zip(aids, top_n_aids, top_n_distances, buckets)]

        # Output dataframe schema.
        schema = StructType([
            StructField("aid", StringType(), True),
            StructField("top_n_similar_user", ArrayType(StructType([StructField('aid', StringType(), False), StructField('score', FloatType(), False)]), True)),
            StructField("aid_bucket", IntegerType(), True)
        ])

        # Create the output dataframe with the similar users for each user.
        df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(data), schema)
        end_format = time.time()
        total_format_time += end_format-end_search

        # Write the output dataframe to Hive.
        print('Writing output to Hive')
        write_to_table_with_partition(df, similarity_table, partition=('aid_bucket'), mode=mode)
        mode = 'append'
        end_write = time.time()
        total_write_time += end_write-end_format
        start_load = end_write

    search_time = time.time()
    print('Index size:', gpu_index.ntotal)
    print(gpu_index.d)
    print(4 * gpu_index.d * gpu_index.ntotal, 'bytes (uncompressed)')
    print('Total time:           ', str(datetime.timedelta(seconds=search_time-start_time)))
    print(' Index load time:     ', str(datetime.timedelta(seconds=load_time-start_time)))
    print(' Overall search time: ', str(datetime.timedelta(seconds=search_time-load_time)))
    print('   Total load time:   ', str(datetime.timedelta(seconds=total_load_time)))
    print('   Total search time: ', str(datetime.timedelta(seconds=total_search_time)))
    print('   Total format time: ', str(datetime.timedelta(seconds=total_format_time)))
    print('   Total write time:  ', str(datetime.timedelta(seconds=total_write_time)))


if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description=" ")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)
        resolve_placeholder(cfg)

    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark_session.sparkContext.setLogLevel('WARN')
    run(spark_session, cfg)

    print('Runtime of the program is:', str(datetime.timedelta(seconds=time.time() - start)))
