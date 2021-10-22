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

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from operator import add
import time
from datetime import datetime, timedelta


"""
Author: Eric Tsai

This script reads the fact data Hive table and print the impression totals 
for all the uckeys.
"""

def get_day_list(yesterday, prepare_past_days):
    day = datetime.strptime(yesterday, '%Y-%m-%d')
    day_list = []
    for _ in range(0, prepare_past_days):
        day_list.append('\"{}\"'.format(datetime.strftime(day, '%Y-%m-%d')))
        day = day + timedelta(days=-1)
    day_list.sort()
    return day_list

def run(cfg):
    start_time = time.time()
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    fact_data_table = cfg['fact_data_table']

    num_buckets = cfg['num_buckets']
    bucket_increment = cfg['bucket_increment']

    yesterday = cfg['yesterday']
    prepare_past_days = cfg['prepare_past_days']
    day_list = get_day_list(yesterday, prepare_past_days)
    # day_list = [ '\"2020-04-01\"' ]

    total = 0
    for i in range(0, num_buckets, bucket_increment):
        print('Bucket {} of {}: {}'.format(i+1, num_buckets, fact_data_table))

        # Load the distribution detail table.
        command = 'select count_array, day, bucket_id from {} where day in ({}) and bucket_id between {} and {}'.format(
            fact_data_table, ','.join(day_list), i, i + bucket_increment)
        # print(command)
        df = hive_context.sql(command)

        _udf = udf(
            lambda x: sum([int(list(i.split(':'))[1]) for i in x if i and ':' in i]) if x else 0, IntegerType())
        df = df.withColumn('imp', _udf(df.count_array))
        # df.show(10)

        increment = df.rdd.map(lambda x: (1,x['imp'])).reduceByKey(add).collect()
        # print('increment: {}'.format(increment))
        total += increment[0][1]

        # Print the results.
        print('Total impressions: {:>16,}'.format(total))
        lap_time = int(time.time() - start_time)
        print('Total elapsed time: {}m {:02}s'.format(lap_time/60, lap_time%60))

    sc.stop()


if __name__ == '__main__':

    cfg = {
        'fact_data_table': 'dlpm_05182021_1500_tmp_area_map',
        'yesterday': '2020-05-31',
        'prepare_past_days': 90,
        'num_buckets': 10,
        'bucket_increment': 1,
        'log_level': 'WARN'
    }

    run(cfg)
