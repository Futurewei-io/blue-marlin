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
|user-1-did| [similarity-score-11, similarity-score-12, similarity-score-13] |[user-did-1, user-did-2, user-did-3]|
|user-2-did| [similarity-score-21, similarity-score-22, similarity-score-23] |[user-did-10, user-did-20, user-did-30]|
|user-3-did| [similarity-score-31, similarity-score-32, similarity-score-33] |[user-did-23, user-did-87, user-did-45]|

'''


def load_score_vectors(spark_session, score_vector_table,
                       bucket, bucket_step, bucket_size):
    # Run the query of the Hive data.
    command = "SELECT did, did_bucket, score_vector FROM {} WHERE did_bucket BETWEEN {} AND {}".format(
        score_vector_table, bucket, min(bucket+bucket_step-1, bucket_size))
    df = spark_session.sql(command)

    # Get the dids, score vectors, and buckets as numpy arrays.

    # we need to collect them first and then seperate them, otherwise we cannot relate dids to scors
    _all = df.select('did', 'score_vector', 'did_bucket').collect()
    dids = np.array([_['did'] for _ in _all])
    score_vectors = np.array([_['score_vector'] for _ in _all], dtype='f4')
    buckets = np.array([_['did_bucket'] for _ in _all])

    # Return the dids and score_vectors.
    return (dids, score_vectors, buckets)


def run(spark_session, cfg):
    score_vector_table = cfg['score_vector']['score_vector_table']
    similarity_table = cfg['top_n_similarity']['similarity_table']
    top_n_value = cfg['top_n_similarity']['top_n']
    did_bucket_size = cfg['top_n_similarity']['did_bucket_size']
    load_bucket_step = cfg['top_n_similarity']['load_bucket_step']
    search_bucket_step = cfg['top_n_similarity']['search_bucket_step']
    index_factory_string = cfg['top_n_similarity']['index_factory_string']

    # If the number of GPUs is 0, uninstall faiss-cpu.
    num_gpus = faiss.get_num_gpus()
    assert num_gpus != 0
    print('Number of GPUs available: {}'.format(num_gpus))

    start_time = time.time()

    # Load the score vectors into the index.
    did_list = []
    for did_bucket in range(0, did_bucket_size, load_bucket_step):
        print('Loading buckets {} - {} of {}'.format(did_bucket, did_bucket+load_bucket_step-1, did_bucket_size))
        (dids, score_vectors, _) = load_score_vectors(spark_session, score_vector_table,
                                                    did_bucket, load_bucket_step, did_bucket_size)

        # Keep track of the dids.
        if did_bucket == 0:
            did_list = dids
        else:
            did_list = np.concatenate((did_list, dids))

        # Create the FAISS index on the first iteration.
        if did_bucket == 0:
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
    for did_bucket in range(0, did_bucket_size, search_bucket_step):
        # Search for the top N similar users for bucket.
        (dids, score_vectors, buckets) = load_score_vectors(spark_session, score_vector_table, did_bucket, search_bucket_step, did_bucket_size)
        end_load = time.time()
        total_load_time += end_load-start_load

        top_n_distances, top_n_indices = gpu_index.search(score_vectors, top_n_value)
        end_search = time.time()
        total_search_time += end_search-end_load

        # start_load = end_search
        # continue

        # Get the top N dids from the top N indexes.
        top_n_dids = did_list[top_n_indices]

        # Format and write the result back to Hive.
        # Format the data for a Spark dataframe in order to write to Hive.
        #  [ ('0000001', [{'did':'0000001', 'score':1.73205081}, {'did':'0000003', 'score':1.73205081}, {'did':'0000004', 'score':0.88532267}, {'did':'0000002', 'score':0.66903623}], 0),
        #    ('0000002', [{'did':'0000002', 'score':1.73205081}, {'did':'0000004', 'score':1.50844401}, {'did':'0000001', 'score':0.66903623}, {'did':'0000003', 'score':0.66903623}], 0),
        #    ... ]
        data = [(str(did), [(str(n_did), float(distance)) for n_did, distance in zip(top_did, top_distances)], int(bucket))
                for did, top_did, top_distances, bucket in zip(dids, top_n_dids, top_n_distances, buckets)]


        # Output dataframe schema.
        schema = StructType([
            StructField("did", StringType(), True),
            StructField("top_n_similar_user", ArrayType(StructType([StructField('did', StringType(), False), StructField('score', FloatType(), False)]), True)),
            StructField("did_bucket", IntegerType(), True)
        ])

        # Create the output dataframe with the similar users for each user.
        df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(data), schema)
        end_format = time.time()
        total_format_time += end_format-end_search

        # Write the output dataframe to Hive.
        write_to_table_with_partition(df, similarity_table, partition=('did_bucket'), mode=mode)
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
