# -*- coding: UTF-8 -*-

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

"""
This script, sum the impression for a traffic from factdata table.

Run like this.

# spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G imp_of_traffic.py
"""


import sys
from datetime import datetime, timedelta
import yaml

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf, expr, collect_list, struct, explode
from pyspark.sql.types import StringType, ArrayType, MapType, FloatType, StructField, StructType, IntegerType


def sum_count_array(count_array):
    '''
    ["2:28","1:15"]
    '''
    count = 0
    for item in count_array:
        _, value = item.split(':')
        count += int(value)

    return count


def run(cfg):
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    # Reading the max bucket_id
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata = cfg['factdata_table']

    start_bucket = 0

    total_count = 0

    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = """
        SELECT
        FACTDATA.count_array,
        FACTDATA.day,
        FACTDATA.hour,
        FACTDATA.uckey
        FROM {} AS FACTDATA
        WHERE FACTDATA.bucket_id BETWEEN {} AND {} 
        AND FACTDATA.day='{}'
        """.format(factdata, str(start_bucket), str(end_bucket), str(cfg['target_day']))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)

        # [Row(count_array=[u'1:504'], day=u'2019-11-02', hour=2, uckey=u'magazinelock,04,WIFI,g_m,1,CPM,78'
        # Extract traffic attributes from uckey based on arritbutes in traffic
        uckey_attrs = cfg['uckey_attrs']
        for attr_index in range(len(uckey_attrs)):
            if (uckey_attrs[attr_index] in cfg['traffic']):
                df = df.withColumn(uckey_attrs[attr_index], udf(lambda x: x.split(',')[attr_index], StringType())(df.uckey))

        # e.g. [Row(uckey=u'magazinelock,01,2G,,,CPM,13', ...
        # m=u'magazinelock', si=u'01', t=u'2G', g=u'', a=u'', pm=u'CPM', r=u'13')]
        # Filter uckey based on traffic
        for attr, attr_value in cfg['traffic'].items():
            df = df.filter(df[attr] == str(attr_value))

        # Extract imp-count from count_array
        # [Row(uckey='native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76', day='2019-11-02', hour=19, count_array=[u'1:504'])]
        df = df.withColumn('count', udf(sum_count_array, IntegerType())(df.count_array))

        l = df.agg({'count': 'sum'}).take(1)
        if (len(l) > 0):
            total_count += l[0]["sum(count)"]

    sc.stop()

    print('FINAL RESULT = {}'.format(str(total_count)))


if __name__ == '__main__':

    cfg = {'factdata_table': 'factdata_hq_09222020_r_ipl_mapped_11052020',
           'bucket_size': 10,
           'bucket_step': 2,
           'log_level': 'INFO',
           'uckey_attrs': ['m', 'si', 't', 'g', 'a', 'pm', 'r', 'ipl'],
           'target_day': '2020-06-01',
           'traffic': {'si': '66bcd2720e5011e79bc8fa163e05184e'}
           }

    run(cfg)
