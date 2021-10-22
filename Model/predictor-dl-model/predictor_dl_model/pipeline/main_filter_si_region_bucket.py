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
import statistics
import yaml
import argparse

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum, array, split
from pyspark.sql.types import BooleanType, IntegerType, StringType
from pyspark.sql import HiveContext
from util import resolve_placeholder
import hashlib


def __save_as_table(df, table_name, hive_context, create_table):

    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

        command = """
            CREATE TABLE IF NOT EXISTS {}
            (
            uckey string, count_array array<string>, hour int, day string, old_bkid int
            ) PARTITIONED BY (bucket_id int)
            """.format(table_name)

        hive_context.sql(command)

    df.select('uckey',
              'count_array',
              'hour',
              'day',
              'old_bkid',
              'bucket_id'
              ).write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)


def drop_residency(df):
    new_uckey = udf(lambda uckey: ','.join([v for i, v in enumerate(uckey.split(',')) if i != 6]))
    df = df.withColumn('_uckey', new_uckey(df.uckey)).drop('uckey').withColumnRenamed('_uckey', 'uckey')
    return df


def modify_ipl(df, mapping):
    def __udf_method(_uckey_str):
        uckey = list(_uckey_str.split(','))
        uckey_ipl = uckey[-1]
        if uckey_ipl in mapping:
            uckey_ipl = mapping[uckey_ipl]
            uckey[-1] = uckey_ipl
        uckey_new = ','.join(uckey)
        return uckey_new

    new_uckey = udf(__udf_method, StringType())
    df = df.withColumn('uckey', new_uckey(df.uckey))
    df = df.drop('virtual').drop('original')
    return df


def modify_residency(df, mapping_df):
    df = df.withColumn('original', split(df['uckey'], ',').getItem(6).cast(IntegerType()))
    df = df.join(mapping_df, on=['original'], how='left')

    def __udf_method(_uckey_str, _r, _virtual_region):
        uckey = list(_uckey_str.split(','))
        uckey_residency = uckey[:-2]
        uckey_ipl = uckey[-1]
        new_residency = _virtual_region
        uckey_new = ','.join(uckey_residency + ['' if new_residency is None else new_residency] + [uckey_ipl])
        return uckey_new

    new_uckey = udf(__udf_method, StringType())
    df = df.withColumn('uckey', new_uckey(df.uckey, df.original, df.virtual))
    df = df.drop('virtual').drop('original')
    return df


def assign_new_bucket_id(df, n):
    def __hash_sha256(s):
        hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
        return int(hex_value, 16)
    _udf = udf(lambda x: __hash_sha256(x) % n)
    df = df.withColumnRenamed('bucket_id', 'old_bkid')
    df = df.withColumn('bucket_id', _udf(df.uckey))
    return df


def run(hive_context, conditions, factdata_table_name, output_table_name, region_mapping_table, init_start_bucket, bucket_size, bucket_step, new_bucket_size, new_si_set):

    # ts will be counts from yesterday-(past_days) to yesterday
    mapping_df = hive_context.sql('SELECT old AS original, new AS virtual FROM {}'.format(region_mapping_table))
    mapping_list = mapping_df.collect()
    mapping = {}
    for row in mapping_list:
        original = row['original']
        virtual = row['virtual']
        mapping[original] = virtual

    start_bucket = init_start_bucket
    first_round = True

    while True:

        end_bucket = min(bucket_size, start_bucket + bucket_step)

        if start_bucket > end_bucket:
            break
        # Read factdata table
        command = """
        SELECT count_array,day,hour,uckey,bucket_id FROM {} WHERE bucket_id BETWEEN {} AND {}
        """.format(factdata_table_name, str(start_bucket), str(end_bucket))

        if len(conditions) > 0:
            command = command + " AND {}".format(' AND '.join(conditions))

        start_bucket = end_bucket + 1

        df = hive_context.sql(command)
        # [Row(count_array=[u'0:0', u'1:0', u'2:0', u'3:0'], day=u'2018-03-09', hour=0, uckey=u'banner,1,3G,g_f,1,pt,1002,icc')]

        # filter good si
        _udf = udf(lambda x: x.split(',')[1] in new_si_set, BooleanType())
        df = df.filter(_udf(df.uckey))

        df = modify_ipl(df, mapping)
        # df = modify_residency(df, mapping_df)

        df = assign_new_bucket_id(df, new_bucket_size)

        df = df.repartition(200)

        # Writing into partitions might throw some exceptions but does not impair data.
        __save_as_table(df, output_table_name, hive_context, first_round)
        print('Processed ' + str(start_bucket - 1) + ' buckets.')

        first_round = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    #  spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G main_filter_si_region_bucket.py &

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log']['level'])

    hive_context.setConf("hive.exec.dynamic.partition", "true")
    hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    cfg_filter = cfg['pipeline']['filter']

    factdata_table_name = cfg['factdata_table_name']
    output_table_name = cfg_filter['output_table_name']
    region_mapping_table = cfg_filter['region_mapping_table']
    init_start_bucket = cfg_filter['init_start_bucket']
    bucket_size = cfg_filter['bucket_size']
    bucket_step = cfg_filter['bucket_step']
    new_bucket_size = cfg_filter['new_bucket_size']
    conditions = cfg_filter['condition']
    new_si_list = cfg_filter['new_si_list']

    new_si_set = set(new_si_list)

    run(hive_context=hive_context, conditions=conditions, factdata_table_name=factdata_table_name,
        output_table_name=output_table_name, region_mapping_table=region_mapping_table, init_start_bucket=init_start_bucket,
        bucket_size=bucket_size, bucket_step=bucket_step, new_bucket_size=new_bucket_size, new_si_set=new_si_set)

    sc.stop()
