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

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode
from pyspark.sql import HiveContext

import yaml
import argparse

import predictor_dl_model.pipeline.prepare_data as prepare_data


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    parser.add_argument('steps')
    args = parser.parse_args()

    pipeline_steps = eval(args.steps)

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    cfg_log = cfg['log']
    cfg = cfg['pipeline']

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg_log['level'])

    yesterday = cfg['yesterday']
    prepare_past_days = cfg['prepare_past_days']
    tmp_table_name = cfg['tmp_table_name']
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']
    factdata_table_name = cfg['factdata_table_name']
    tf_statistics_path = cfg['tf_statistics_path']
    holidays = cfg['holidays']

    if (1 in pipeline_steps):
        prepare_data.prepare_tfrecords(hive_context,factdata_table_name,
                                       yesterday, prepare_past_days, tmp_table_name, bucket_size, bucket_step, tf_statistics_path, holidays)

    # save table as tfrecords
    path = cfg['tfrecords_hdfs_path']
    command = """
            SELECT * FROM {}
            """.format(tmp_table_name)
    if (2 in pipeline_steps):
        df = hive_context.sql(command)
        df.write.format("tfrecords").option("recordType", "Example").mode('overwrite').save(path)

    sc.stop()
