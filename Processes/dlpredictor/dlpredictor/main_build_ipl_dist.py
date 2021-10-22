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

import math
import pickle
import yaml
import argparse

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum, array, split
from pyspark.sql.types import BooleanType, IntegerType, StringType, FloatType
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
from dlpredictor.configutil import *
import hashlib

'''
spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G dlpredictor/main_build_ipl_dist.py conf/config.yml
'''


def __save_as_table(df, table_name, hive_context, create_table):
    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

        df.createOrReplaceTempView("r900_temp_table")

        command = """
            CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM r900_temp_table
            """.format(table_name)

        hive_context.sql(command)


def run(hive_context, conditions, factdata_table, ipl_dist_table, unique_original_uckey_table, region_mapping_table, bucket_size, bucket_step):

    # ts will be counts from yesterday-(past_days) to yesterday
    mapping_df = hive_context.sql('SELECT old,new FROM {}'.format(region_mapping_table))

    start_bucket = 0
    df_union = None
    df_distinct_uckey = None

    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break

        # Read factdata table
        command = """
        SELECT count_array,uckey,bucket_id FROM {} WHERE bucket_id BETWEEN {} AND {}
        """.format(factdata_table, str(start_bucket), str(end_bucket))

        if len(conditions) > 0:
            command = command + " and {}".format(' and '.join(conditions))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)
        # [Row(count_array=[u'0:0', u'1:0', u'2:0', u'3:0'], day=u'2018-03-09', hour=0, uckey=u'banner,1,3G,g_f,1,pt,1002,icc')]

        # extract ipl
        df = df.withColumn('ipl', split(df['uckey'], ',').getItem(7).cast(StringType()))

        def _udf_helper(count_arrays):
            result = 0
            for count_array in count_arrays:
                for item in count_array:
                    imp = int(item.split(':')[1])
                    result += imp
            return result

        df_uckey = df.select('uckey')
        if df_distinct_uckey is None:
            df_distinct_uckey = df_uckey.select('uckey').distinct()
        else:
            df_distinct_uckey = df_distinct_uckey.union(df_uckey)
            df_distinct_uckey = df_distinct_uckey.select('uckey').distinct()

        df = df.groupby('ipl').agg(udf(_udf_helper, IntegerType())(collect_list('count_array')).alias('imp'))
        if df_union is None:
            df_union = df
        else:
            df_union = df_union.union(df)

    df = df_union.groupby('ipl').agg(sum('imp').alias('region_imp'))
    df = mapping_df.join(df, mapping_df.old == df.ipl, 'outer')
    df = df.withColumn('region_total_imp', sum('region_imp').over(Window.partitionBy('new')))
    df = df.withColumn('ratio', udf(lambda x, y: float(x)/y if x and y else 0, FloatType())('region_imp', 'region_total_imp'))

    __save_as_table(df=df, table_name=ipl_dist_table, hive_context=hive_context, create_table=True)

    __save_as_table(df=df_distinct_uckey, table_name=unique_original_uckey_table, hive_context=hive_context, create_table=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    hive_context.setConf("hive.exec.dynamic.partition", "true")
    hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    factdata_table = cfg['factdata_table']
    region_mapping_table = cfg['region_mapping_table']
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    conditions = cfg['condition']
    ipl_dist_table = cfg['ipl_dist_table']
    unique_original_uckey_table = cfg['unique_original_uckey_table']

    run(hive_context=hive_context, conditions=conditions, factdata_table=factdata_table,
        ipl_dist_table=ipl_dist_table, unique_original_uckey_table=unique_original_uckey_table, region_mapping_table=region_mapping_table,
        bucket_size=bucket_size, bucket_step=bucket_step)

    sc.stop()
