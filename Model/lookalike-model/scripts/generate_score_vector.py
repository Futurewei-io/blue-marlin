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

import random
import time
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType, IntegerType, MapType, StringType, StructField, StructType


def write_to_table_with_partition(df, table_name, partition, mode='overwrite'):
    if mode == 'overwrite':
        df.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').partitionBy(partition).saveAsTable(table_name)
    else:
        df.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').insertInto(table_name)


def create_score_vector_df (spark_session, num_users, dim_of_user_vector, num_did_buckets):

    user_data = []
    for _ in range(num_users):
        did = hex(random.getrandbits(256)).lstrip('0x').rstrip('L'),
        did_bucket = random.randrange(num_did_buckets)
        score_vector = np.random.random((dim_of_user_vector)).astype('float32')
        c1 = float(score_vector.dot(score_vector))
        score_vector = [ float(i) for i in score_vector ]  # Convert from numpy float type.

        user_data.append( (did, score_vector, c1, did_bucket) )
    
    schema = StructType([        
        StructField("did", StringType(), True),
        StructField("score_vector", ArrayType(FloatType()), True),
        StructField("c1", FloatType(), True),
        StructField("did_bucket", IntegerType(), True),
    ])

    return spark_session.createDataFrame(spark_session.sparkContext.parallelize(user_data), schema)


def run(spark_session, cfg):
    num_users = cfg['number_users']
    num_did_buckets = cfg['number_did_buckets']
    increment_size = cfg['increment_size']
    output_table = cfg['output_table']
    len_score_vector = cfg['len_score_vector']
    overwrite = cfg['overwrite']

    start_time = time.time()
    elapsed_times = [ 0 ]
    num_users_generated = 0
    mode = 'append'
    if overwrite:
        mode = 'overwrite'
    while num_users_generated < num_users:
        chunk_size = min(num_users - num_users_generated, increment_size)
        print('Generating users {} to {}'.format(num_users_generated, num_users_generated + chunk_size -1))
        df = create_score_vector_df(spark_session, chunk_size, len_score_vector, num_did_buckets)

        print('Writing users {} to {}'.format(num_users_generated, num_users_generated + chunk_size -1))
        write_to_table_with_partition(df, output_table, partition=('did_bucket'), mode=mode)
        mode = 'append'
        num_users_generated += chunk_size
        elapsed_time = int(time.time() - start_time)
        iteration_time = elapsed_time - elapsed_times[-1]
        elapsed_times.append(elapsed_time)
        print('Elapsed time: {:2}:{:02}:{:02}  Last iteration: {:2}:{:02}  Users generated: {:>13,}'.format(
            elapsed_time/3600, elapsed_time/60%60, elapsed_time%60, iteration_time/60, iteration_time%60, 
            num_users_generated))


if __name__ == '__main__':
    cfg = {
        'number_users': 10000000,
        'increment_size': 500000,
        'number_did_buckets': 100,
        'len_score_vector': 32,
        'output_table': 'lookalike_application_12082021_generated_score_vector',
        'overwrite': True,
    }

    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark_session.sparkContext.setLogLevel('WARN')

    run(spark_session, cfg)
    spark_session.stop()


