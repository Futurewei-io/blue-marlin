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

import yaml
import argparse
import pyspark.sql.functions as fn
from pyspark import SparkContext
from pyspark.sql.functions import udf, lit, col, collect_list, avg, dense_rank, array
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql import HiveContext
from util import resolve_placeholder, hampel
import pandas as pd


def write_to_table(df, table_name, mode='overwrite'):
    df.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').saveAsTable(table_name)


def run(hive_context, input_table_name, outlier_table):

    # Read factdata table
    command = """
    SELECT ts FROM {}
    """.format(input_table_name)

    # DataFrame[ts: array<int>]
    df = hive_context.sql(command)

    columns = df.columns
    df_sizes = df.select(*[fn.size(col).alias(col) for col in columns])
    df_max = df_sizes.agg(*[fn.max(col).alias(col) for col in columns])
    max_dict = df_max.collect()[0].asDict()
    df_result = df.select(*[df[col][i] for col in columns for i in range(max_dict[col])])
    df_result = df_result.na.fill(value=0)
    ts_l = df_result.groupBy().sum().collect()[0]
    ts_l = pd.Series(list(ts_l))
    outlier_indices = hampel(ts_l, window_size=5, n=6)

    def _filter_outlier(x, ind_list):
        for i in range(1, len(x)-1):
            if i in ind_list and x[i] != None and x[i + 1] != None and x[i - 1] != None:
                x[i] = int((x[i - 1] + x[i + 1]) / 2)
        return x

    command = """
        SELECT * FROM {}
        """.format(input_table_name)
    df = hive_context.sql(command)
    df = df.withColumn("indices", array([fn.lit(int(x)) for x in outlier_indices]))
    df = df.withColumn('ts', udf(_filter_outlier, ArrayType(IntegerType()))(df['ts'], df['indices']))
    write_to_table(df, outlier_table, mode='overwrite')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    cfg_log = cfg['log']
    cfg = cfg['pipeline']

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg_log['level'])

    percentile = cfg['filter']['percentile']
    output_table_name = cfg['uckey_clustering']['output_table_name']
    pre_cluster_table_name = cfg['uckey_clustering']['pre_cluster_table_name']
    create_pre_cluster_table = cfg['uckey_clustering']['create_pre_cluster_table']
    input_table_name = cfg['time_series']['output_table_name']
    cluster_size_cfg = cfg['uckey_clustering']['cluster_size']
    outlier_table = cfg['time_series']['outlier_table']

    run(hive_context=hive_context,
        input_table_name=input_table_name,
        outlier_table=outlier_table)

    sc.stop()
