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

# -*- coding: UTF-8 -*-

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf, expr, collect_list, struct
from pyspark.sql.types import StringType, ArrayType, MapType, FloatType, StructField, StructType, BooleanType

"""
Author: Reza

This sript create a new tmp-area table by filtering some SIs.

spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G filter_si_tmp_area.py
"""


def __save_as_table(df, table_name, hive_context, create_table):

    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

        df.createOrReplaceTempView("r907_temp_table")

        command = """
            CREATE TABLE IF NOT EXISTS {} as select * from r907_temp_table
            """.format(table_name)

        hive_context.sql(command)


def filter_si(si_list):
    def _helper(uckey):
        return uckey.split(',')[1] in si_list
    return _helper


def run(cfg):
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    # Reading the max bucket_id
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata = cfg['factdata_table']
    si_list = cfg['si_list']

    start_bucket = 0
    final_df = None

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
        FACTDATA.uckey,
        FACTDATA.bucket_id 
        FROM {} AS FACTDATA
        WHERE FACTDATA.bucket_id BETWEEN {} AND {}
        """.format(factdata, str(start_bucket), str(end_bucket))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)

        # filter df
        df = df.filter(udf(filter_si(si_list), BooleanType())(df.uckey))

        if final_df is None:
            final_df = df
        else:
            final_df = df.unionByName(final_df)

    __save_as_table(df=final_df, table_name=factdata + "_si_filtered", hive_context=hive_context, create_table=True)

    sc.stop()


if __name__ == '__main__':

    cfg = {
        'factdata_table': 'dlpm_06092021_1500_tmp_area_map',
        'bucket_size': 10,
        'bucket_step': 4,
        'log_level': 'WARN',
        'si_list': ['71bcd2720e5011e79bc8fa163e05184e']
    }

    run(cfg)
