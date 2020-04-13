# Baohua Cao
# Testing Optimizer's performance.

# Copyright 2019, Futurewei Technologies
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

import timeit

"""
    testing environment: on machine 10.193.217.105
    cat /proc/cpuinfo, /proc/meminfo
    processor numbers: 39
    for each processor:
        cpu family      : 6
        model name      : Intel(R) Xeon(R) CPU E5-2698 v4 @ 2.20GHz
        cpu MHz         : 1199.859
        cache size      : 51200 KB
    MemTotal:       264065316 kB
    MemFree:        228545848 kB
    MemAvailable:   253897916 kB
    Buffers:         2033780 kB
    Cached:         23581032 kB
"""

if __name__ == '__main__':

    testing_times = 10
    
    SET_UP = """
from imscommon.es.ims_esclient import ESClient
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType, Row
import optimizer.main
import optimizer.algo.hwm
import optimizer.util
import inspect

schema = StructType([StructField('day', StringType(), True),
                    StructField('ands', ArrayType(StringType()), True),
                    StructField('minus', ArrayType(StringType()), True),
                    StructField('allocated', MapType(
                        StringType(), IntegerType()), True),
                    StructField('amount', IntegerType(), True)])
cfg = {
        'es_host': '10.193.217.111',
        'es_port': '9200',
        'es_booking_index': 'bookings_bob20200323',
        'es_booking_type': 'doc',
        'es_predictions_index': 'predictions_01202020',
        'es_predictions_type': 'doc',
        'es_tbr_index': 'tbr_test_10012019',
        'es_tbr_type': 'doc',
        'today': '20180405'
    }

es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                cfg['es_booking_index'], cfg['es_booking_type'])
bookings = es_client_booking.search({"size": 100})
bookings_map = optimizer.util.get_bookings_map(bookings)
sc = SparkContext()
sc.setLogLevel('WARN')
hive_context = HiveContext(sc)

# 1 booking splitted by another booking, total 2 bookings, 2 buckets splitted, 1 valid bucket returned.
def test_split_bbs_1():
    print(inspect.stack()[0][3])
    new_bk_id = 'b6'
    rows = [('20180405', ['b8'], [], {}, 0)]
    df = hive_context.createDataFrame(rows, schema)
    spark_df_splitted_rdd = df.rdd.flatMap(optimizer.main.bb_split_method(cfg, bookings_map, new_bk_id))

# 2 bookings splitted by another booking, total 3 bookings, 4 buckets splitted, 2 valid buckets returned.
def test_split_bbs_2():
    print(inspect.stack()[0][3])
    new_bk_id = 'b7'
    rows = [('20180405', ['b8'], ['b6'], {}, 3239), ('20180405', ['b6'], ['b8'], {}, 0)]
    df = hive_context.createDataFrame(rows, schema)
    spark_df_splitted_rdd = df.rdd.flatMap(optimizer.main.bb_split_method(cfg, bookings_map, new_bk_id))

# 1 booking is allocated.
def test_hwm_allocation_case1():
    print(inspect.stack()[0][3])
    rows = [(['20180405', ['b8'], ['b6'], {}, 3239])]
    df = hive_context.createDataFrame(rows, schema)
    days = optimizer.util.get_days_from_bookings(cfg['today'], bookings)
    df_allocated = optimizer.algo.hwm.hwm_allocation(df, bookings, days)

# 5 bookings are allocated.
def test_hwm_allocation_case2():
    print(inspect.stack()[0][3])
    rows = [(['20180405', ['b6', 'b7', 'b10', 'b11', 'b12'], ['b8', 'b9'],  {}, 8900])]
    df = hive_context.createDataFrame(rows, schema)
    days = optimizer.util.get_days_from_bookings(cfg['today'], bookings)
    df_allocated = optimizer.algo.hwm.hwm_allocation(df, bookings, days)

def test_nothing():
    print(inspect.stack()[0][3])
    pass

    """

    testing_method = "test_hwm_allocation_case2()"
    #test_split_bbs_1(), test_split_bbs_2(), test_nothing(), test_hwm_allocation_case1()
    running_time = timeit.Timer(setup = SET_UP, stmt = testing_method).timeit(testing_times)

    # testing_times is the number executions of the main stmt statement. This executes the setup statement once, and then returns 
    # the time it takes to execute the main statement a number of times, measured in seconds as a float. 

    print('Testing method: ' + str(testing_method) + ', testing times: ' + str(testing_times) + 
    ', the average running time: ' + '{:.3f}'.format(running_time) + 's')
