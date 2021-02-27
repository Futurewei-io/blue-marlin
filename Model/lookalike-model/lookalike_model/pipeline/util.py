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
import pickle
import os

from pyspark import SparkContext, SQLContext
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql import HiveContext
import re


def load_config(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('config_file')
    args = parser.parse_args()
    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    sc = SparkContext.getOrCreate()
    sc.setLogLevel(cfg['log']['level'])
    hive_context = HiveContext(sc)
    return sc, hive_context, cfg


def resolve_placeholder(in_dict):
    stack = []
    for key in in_dict.keys():
        stack.append((in_dict, key))
    while len(stack) > 0:
        (_dict, key) = stack.pop()
        value = _dict[key]
        if type(value) == dict:
            for _key in value.keys():
                stack.append((value, _key))
        elif type(value) == str:
            z = re.findall('\{(.*?)\}', value)
            if len(z) > 0:
                new_value = value
                for item in z:
                    if item in in_dict and type(in_dict[item]) == str:
                        new_value = new_value.replace('{'+item+'}', in_dict[item])
                _dict[key] = new_value


def load_batch_config(cfg):
    cfg_clean = cfg['pipeline']['main_clean']
    start_date = cfg_clean['conditions']['starting_date']
    end_date = cfg_clean['conditions']['ending_date']
    load_minutes = cfg_clean['load_logs_in_minutes']
    return start_date, end_date, load_minutes


def drop_table(hive_context, table_name):
    command = """drop table IF EXISTS {}""".format(table_name)
    hive_context.sql(command)


def create_log_table(hive_context, table_name):
    command = """
            CREATE TABLE IF NOT EXISTS {}
            (
            did string,
            spread_app_id string,
            adv_id string,
            media string,
            slot_id string,
            device_name string,
            net_type string,
            price_model string,
            action_time string,
            media_category string,
            keyword string,
            keyword_index int,
            gender int,
            age int
            )
            """.format(table_name)
    hive_context.sql(command)


def load_df(hive_context, table_name):
    command = """select * from {}""".format(table_name)
    return hive_context.sql(command)


def write_to_table(df, table_name, mode='overwrite'):
    df.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').saveAsTable(table_name)


def write_to_table_with_partition(df, table_name, partition, mode='overwrite'):
    if mode == 'overwrite':
        df.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').partitionBy(partition).saveAsTable(table_name)
    else:
        df.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').insertInto(table_name)


def index_distinct_df(df_distinct, column_name, column_index_name):
    # use rdd.zipWithIndex to generate auto index starting from 0 for all rows of distinct df.
    df_distinct = df_distinct.rdd.zipWithIndex()
    df_distinct = df_distinct.toDF()
    df_distinct = df_distinct.withColumn(column_name, df_distinct["_1"].getItem(column_name))
    df_distinct = df_distinct.withColumn(column_index_name, df_distinct["_2"])
    df_distinct = df_distinct.drop(df_distinct._1).drop(df_distinct._2)
    # starting index from 1 for training the din model well.
    df_distinct = df_distinct.withColumn(column_index_name, udf(lambda x: x+1, IntegerType())(col(column_index_name)))
    return df_distinct

# read keywords list from a keywords data file.


def read_ad_keywords_list(ad_keywords_list_file_name):
    ad_keywords_list = []
    fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    # the ad keywords file should be put under the "data" folder.
    with open(fpath + ad_keywords_list_file_name, 'r') as fp:
        line = fp.readline()
        while line:
            # transform different spaces to one space in each line.
            line = " ".join(line.split())
            # extract spread_app_id, keyword from the file.
            ad_keywords_list.append(line.split(" "))
            line = fp.readline()
    return ad_keywords_list

# generate keywords table on demand from a keywords list.


def generate_add_keywords(keywords_table, keywords_file_path):
    # generate json keyword rows, create df and save to table.
    ad_keywords_list_file_name = keywords_file_path
    ad_keywords_list = read_ad_keywords_list(ad_keywords_list_file_name)
    keyword_rows = []
    for kw_line in ad_keywords_list:
        keyword_dict = dict(
            zip(['spread_app_id', 'keyword'], [kw_line[0], kw_line[1]]))
        keyword_rows.append(keyword_dict)
    # transform the keywords to df and save to hive table.
    sc = SparkContext.getOrCreate()
    sql_context = SQLContext(sc)
    df_keywords = sql_context.read.json(sc.parallelize(keyword_rows))
    df_keywords = df_keywords.withColumn('index_id', monotonically_increasing_id())
    write_to_table(df_keywords, keywords_table)
    return df_keywords


def save_pickle_file(python_object, file_path):
    file_output = open(file_path, 'wb')
    pickle.dump(python_object, file_output)
    file_output.close()


def print_batching_info(process, batch_number, time_start, time_end):
    print(process + ': batch ' + str(batch_number) +
          ' is processed in time range ' + time_start + ' - ' + time_end)
