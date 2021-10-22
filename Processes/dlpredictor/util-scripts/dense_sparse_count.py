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
import pyspark.sql.functions as functions
from operator import add
import time

"""
Author: Eric Tsai

This script reads the *_tmp_ts and *_distribution_detail Hive tables and print the
impression totals for all the uckeys, the dense uckeys, and the sparse ones.
"""

def run(cfg):
    start_time = time.time()

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    # Load the distribution detail table.
    distribution_table = cfg['distribution_table']
    command = 'select * from {}'.format(distribution_table)
    df = hive_context.sql(command)
    df = df.select('uckey', 'price_cat', 'ratio')
    df.cache()

    # Get the dense uckeys.
    df_dense = df.filter(df['ratio'] == 1)

    # Get the sparse uckeys.
    df_sparse = df.filter(df['ratio'] != 1)

    # Read the time series tables and unpack the time series column and
    # filter out the nulls.
    time_series_table = cfg['time_series_table']
    command = 'select * from {}'.format(time_series_table)
    df_ts = hive_context.sql(command)
    df_ts = df_ts.withColumn('day_imp', functions.explode(functions.col('ts')))
    df_ts = df_ts.filter(df_ts.day_imp.isNotNull())
    df_ts.select('uckey', 'price_cat', 'day_imp')
    df_ts.cache()

    # Calculate the total impressions in the time series table.
    total = df_ts.rdd.map(lambda x: (1,x['day_imp'])).reduceByKey(add).collect()[0][1]
    print('Total: {:>15,}'.format(total))
    lap_time = int(time.time() - start_time)
    print('Total elapsed time: {}:{:02}'.format(lap_time/60, lap_time%60))

    # Calculate the total number of impressions in the dense (non-sparse) uckeys.
    df_ts_dense = df_ts.join(df_dense, on=[ 'uckey', 'price_cat' ], how='inner')
    dense_total = df_ts_dense.rdd.map(lambda x: (1,x['day_imp'])).reduceByKey(add).collect()[0][1]
    print('Dense impression total: {:>15,}'.format(dense_total))
    lap_time = int(time.time() - start_time)
    print('Total elapsed time: {}:{:02}'.format(lap_time/60, lap_time%60))

    # Calculate the total number of impressions of sparse uckeys.
    df_ts_sparse = df_ts.join(df_sparse, on=[ 'uckey', 'price_cat' ], how='inner')
    sparse_total = df_ts_sparse.rdd.map(lambda x: (1,x['day_imp'])).reduceByKey(add).collect()[0][1]
    print('Sparse impression total: {:>15,}'.format(dense_total))
    lap_time = int(time.time() - start_time)
    print('Total elapsed time: {}:{:02}'.format(lap_time/60, lap_time%60))

    df_ts_other = df_ts.join(df_dense, on=[ 'uckey', 'price_cat' ], how='left_anti')
    df_ts_other = df_ts_other.join(df_sparse, on=[ 'uckey', 'price_cat' ], how='left_anti')
    df_ts_other.select('day_imp')
    other_total = df_ts_other.rdd.map(lambda x: (1,x['day_imp'])).reduceByKey(add).collect()[0][1]
    lap_time = int(time.time() - start_time)
    print('Total elapsed time: {}:{:02}'.format(lap_time/60, lap_time%60))

    sc.stop()

    # Print the results.
    print('Reading data from tables: {} and {}'.format(time_series_table, distribution_table))
    print('Dense impression total:       {:>15,}'.format(dense_total))
    print('Sparse impression total:      {:>15,}'.format(sparse_total))
    print('Other impression total:       {:>15,}'.format(other_total))
    print('Dense + sparse total:         {:>15,}'.format(dense_total + sparse_total))
    print('Dense + sparse + other total: {:>15,}'.format(dense_total + sparse_total + other_total))
    print('Total impressions in table:   {:>15,}'.format(total))
    print('Dense impression %:  {:>5.1f}%'.format(float(dense_total)*100/(dense_total + sparse_total)))
    print('Sparse impression %: {:>5.1f}%'.format(float(sparse_total)*100/(dense_total + sparse_total)))


if __name__ == '__main__':

    cfg = {
        # 'distribution_table': 'dlpm_03182021_tmp_distribution_detail',
        # 'time_series_table': 'dlpm_03182021_tmp_ts',
        'distribution_table': 'dlpm_05182021_1500_tmp_distribution_detail',
        'time_series_table': 'dlpm_05182021_1500_tmp_ts',
        'log_level': 'WARN'
    }

    run(cfg)
