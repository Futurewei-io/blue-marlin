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

from pyspark import SparkContext, SQLContext, Row
from pyspark.sql.types import ArrayType, FloatType, IntegerType, MapType, StringType, StructField, StructType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql import HiveContext

from lookalike_model.pipeline.util import write_to_table_with_partition, load_config, resolve_placeholder



'''

spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict generate_users.py config_generate_users.yml

'''


def create_user_df (hive_context, num_users, num_time_intervals, num_did_buckets):
    time_intervals = [1586822400 - i*86400 for i in range(num_time_intervals)]
    keyword_list = [
        "education", "entertainment", "game-act", "game-avg", "game-cnc", 
        "game-ent", "game-fishing", "game-moba", "game-mon", "game-rpg", 
        "game-sim", "game-slg", "health", "info", "living-car", 
        "living-food", "living-house", "living-insurance", "living-makeup", "living-map", 
        "living-mon", "living-photo", "other", "reading", "shopping", 
        "social", "sports", "travel", "video", 
    ]
    keyword_index_map = {
        "education": 1, "entertainment": 2, "game-act": 3, "game-avg": 4, "game-cnc": 5, 
        "game-ent": 6, "game-fishing": 7, "game-moba": 8, "game-mon": 9, "game-rpg": 10, 
        "game-sim": 11, "game-slg": 12, "health": 13, "info": 14, "living-car": 15, 
        "living-food": 16, "living-house": 17, "living-insurance": 18, "living-makeup": 19, "living-map": 20, 
        "living-mon": 21, "living-photo": 22, "other": 23, "reading": 24, "shopping": 25, 
        "social": 26, "sports": 27, "travel": 28, "video": 29, 
    }

    user_data = []
    for i in range(num_users):
        keyword_samples = [random.sample(keyword_list, random.randint(1,3)) for _ in range(num_time_intervals)]
        kws_sample = random.sample(keyword_list, int(0.9*len(keyword_list)))
        show_counts = [[int(random.expovariate(1))+1 for _ in keywords] for keywords in keyword_samples]
        user_data.append(Row(
            age = random.randint(1, 6), 
            gender = random.randint(0, 1), 
            did = hex(random.getrandbits(256)).lstrip('0x').rstrip('L'),
            did_index = i, 
            interval_start_time = time_intervals,
            interval_keywords = [','.join(keyword) for keyword in keyword_samples],
            kwi = [','.join([str(keyword_index_map[keyword]) for keyword in keywords]) for keywords in keyword_samples], 
            kwi_show_counts = [','.join('{}:{}'.format(keyword_index_map[keyword], count) for count, keyword in zip(show_count, keywords)) for show_count, keywords in zip(show_counts, keyword_samples)], 
            kwi_click_counts = [','.join('{}:{}'.format(keyword_index_map[keyword], count - random.randint(0, count)) for count, keyword in zip(show_count, keywords)) for show_count, keywords in zip(show_counts, keyword_samples)], 
            kws = {keyword: random.random() for keyword in kws_sample},
            kws_norm = {keyword: random.random() for keyword in kws_sample},
            did_bucket = random.randrange(num_did_buckets)
        ))

    schema = StructType([        
        StructField("age", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("did", StringType(), True),
        StructField("did_index", IntegerType(), True),
        StructField("interval_start_time", ArrayType(StringType()), True),
        StructField("interval_keywords", ArrayType(StringType()), True),
        StructField("kwi", ArrayType(StringType()), True),
        StructField("kwi_show_counts", ArrayType(StringType()), True),
        StructField("kwi_click_counts", ArrayType(StringType()), True),
        StructField("kws", MapType(StringType(), FloatType()), True),
        StructField("kws_norm", MapType(StringType(), FloatType()), True),
        StructField("did_bucket", IntegerType(), True),
    ])

    return hive_context.createDataFrame(user_data, schema)


def run(sc, hive_context, cfg):
    num_users = cfg['generate_users']['number_users']
    num_time_intervals = cfg['generate_users']['number_time_intervals']
    num_did_buckets = cfg['generate_users']['number_did_buckets']
    increment_size = cfg['generate_users']['increment_size']
    output_table = cfg['generate_users']['output_table']

    start_time = time.time()
    elapsed_times = [ 0 ]
    num_users_generated = 0
    # mode = 'overwrite'
    mode = 'overwrite'
    while num_users_generated < num_users:
        chunk_size = min(num_users - num_users_generated, increment_size)
        df = create_user_df(hive_context, chunk_size, num_time_intervals, num_did_buckets)
        write_to_table_with_partition(
            df.select('age', 'gender', 'did', 'did_index', 'interval_start_time', 'interval_keywords', 
                'kwi', 'kwi_show_counts', 'kwi_click_counts', 'kws', 'kws_norm', 'did_bucket'), 
            output_table, partition=('did_bucket'), mode=mode)
        mode = 'append'
        num_users_generated += chunk_size
        elapsed_time = int(time.time() - start_time)
        iteration_time = elapsed_time - elapsed_times[-1]
        elapsed_times.append(elapsed_time)
        print('Elapsed time: {:2}:{:02}:{:02}  Last iteration: {:2}:{:02}  Users generated: {:>13,}'.format(
            elapsed_time/3600, elapsed_time/60%60, elapsed_time%60, iteration_time/60, iteration_time%60, 
            num_users_generated))


if __name__ == '__main__':
    sc, hive_context, cfg = load_config(description="")
    resolve_placeholder(cfg)
    run(sc, hive_context, cfg)
    sc.stop()

