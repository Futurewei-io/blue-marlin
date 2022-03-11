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

import yaml
import argparse

from pyspark import SparkContext
from pyspark.sql.functions import lit, udf
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType, ArrayType
from datetime import datetime, timedelta
from util import resolve_placeholder
import hashlib


'''
This module transform 
T1 : request based factdata
T2 : compatible factdata for main_ts.py

T1

+-----------------------+---------+-------+
|col_name               |data_type|comment|
+-----------------------+---------+-------+
|uckey                  |string   |null   |
|total                  |int      |null   |
|pt_d                   |string   |null   |
|# Partition Information|         |       |
|# col_name             |data_type|comment|
|pt_d                   |string   |null   |
+-----------------------+---------+-------+
CREATE TABLE table_name ( uckey string, total int)                                
PARTITIONED BY (pt_d string)



T2


+-----------------------+-------------+-------+
|col_name               |data_type    |comment|
+-----------------------+-------------+-------+
|uckey                  |string       |null   |
|count_array            |array<string>|null   |
|hour                   |int          |null   |
|day                    |string       |null   |
|bucket_id              |int          |null   |
|# Partition Information|             |       |
|# col_name             |data_type    |comment|
|day                    |string       |null   |
|bucket_id              |int          |null   |
+-----------------------+-------------+-------+


spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/main_rti_transform.py config.yml

'''


def advance_date(date, day_added):
    _time = datetime.strptime(date, "%Y-%m-%d")
    _time = _time + timedelta(days=day_added)
    return _time.strftime("%Y-%m-%d")


def assign_new_bucket_id(df, n):
    def __hash_sha256(s):
        hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
        return int(hex_value, 16)
    _udf = udf(lambda x: __hash_sha256(x) % n)
    df = df.withColumn('bucket_id', _udf(df.uckey))
    return df


def __save_as_table(df, table_name, hive_context, create_table):

    if create_table:
        command = """
            DROP TABLE IF EXISTS {}
            """.format(table_name)

        hive_context.sql(command)

        command = """
            CREATE TABLE IF NOT EXISTS {}
            (
            uckey string, count_array array<string>, hour int, day string 
            ) PARTITIONED BY (bucket_id int)
            """.format(table_name)

        hive_context.sql(command)

    df.select('uckey',
              'count_array',
              'hour',
              'day',
              'bucket_id'
              ).write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)


def run(hive_context,
        input_table, output_table,
        start_day, end_day, day_step,
        new_bucket_size,
        default_hour, default_price_cat):

    _st = start_day
    first_round = True

    while True:

        _end = min(end_day, advance_date(_st, day_step))

        if _st > _end:
            break

        # Read factdata table
        command = """
        SELECT uckey, total, pt_d FROM {} WHERE pt_d BETWEEN '{}' AND '{}'
        """.format(input_table, _st, _end)

        _st = advance_date(_end, 1)

        df = hive_context.sql(command)
        print(command)

        df = df.withColumnRenamed('pt_d', 'day')

        # add count_array
        # [Row(count_array=[u'0:0', u'1:0', u'2:0', u'3:0'], day=u'2018-03-09', hour=0, uckey=u'banner,1,3G,g_f,1,pt,1002,icc')]
        df = df.withColumn('count_array', udf(lambda x: [default_price_cat + ':' + str(x)], ArrayType(StringType()))(df.total))

       # add hour
        df = df.withColumn('hour', lit(default_hour))

        df = assign_new_bucket_id(df, new_bucket_size)

        # we want to keep the paritions for a batch under 200
        df = df.repartition(200)

        # Writing into partitions might throw some exceptions but does not impair data.
        __save_as_table(df, output_table, hive_context, first_round)

        first_round = False

    return


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    cfg_log = cfg['log']
    cfg_rti = cfg['pipeline']['rti_transform']

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg_log['level'])

    default_hour = cfg_rti['default_hour']
    default_price_cat = cfg_rti['default_price_cat']
    day_step = cfg_rti['day_step']
    start_day = cfg_rti['start_day']
    end_day = cfg_rti['end_day']
    new_bucket_size = cfg_rti['new_bucket_size']
    input_table = cfg_rti['input_table']
    output_table = cfg['factdata_table_name']

    run(hive_context=hive_context,
        input_table=input_table, output_table=output_table,
        start_day=start_day, end_day=end_day, day_step=day_step,
        new_bucket_size=new_bucket_size,
        default_hour=default_hour, default_price_cat=default_price_cat)

    sc.stop()
