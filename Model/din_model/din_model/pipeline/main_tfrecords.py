# Copyright 2020, Futurewei Technologies
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
from pyspark.sql import HiveContext
from util import load_df, save_pickle_file


def generate_tf_statistics(df, tfrecords_statistics_path):
    tfrecords_statistics = {}
    tfrecords_statistics['distinct_records_count'] = df.count()
    save_pickle_file(tfrecords_statistics, tfrecords_statistics_path)


def save_tfrecords(trainready_table_name, tfrecords_hdfs_path, tfrecords_statistics_path):
    command = """select uckey_index, media_index, media_category_index, net_type_index, gender_index, age_index, 
    region_id_index, interval_starting_time, keyword_indexes as keywords, keyword_indexes_click_counts as click_counts, 
    keyword_indexes_show_counts as show_counts from {}""".format(trainready_table_name)
    df = hive_context.sql(command)
    generate_tf_statistics(df, tfrecords_statistics_path)
    df.write.format("tfrecords").option("recordType", "Example").mode('overwrite').save(tfrecords_hdfs_path)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='generate tf records')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log']['level'])

    # save table as tfrecords
    cfg_pipeline = cfg['pipeline']
    trainready_table_name = cfg_pipeline['main_trainready']['trainready_output_table_name']
    tfrecords_hdfs_path = cfg_pipeline['tfrecords']['tfrecords_hdfs_path']
    tfrecords_statistics_path = cfg_pipeline['tfrecords']['tfrecords_statistics_path']
    save_tfrecords(trainready_table_name, tfrecords_hdfs_path, tfrecords_statistics_path)

    sc.stop()
