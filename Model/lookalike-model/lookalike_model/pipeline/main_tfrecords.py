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

from pyspark import SparkContext
from pyspark.sql import HiveContext
from util import load_config, load_df, save_pickle_file, resolve_placeholder


def generate_tf_statistics(df, tf_statis_path):
    tfrecords_statistics = {}
    tfrecords_statistics['distinct_records_count'] = df.count()
    save_pickle_file(tfrecords_statistics, tf_statis_path)


def save_tfrecords(hive_context, trainready_table, tfrecords_hdfs_path, tf_statis_path):
    command = """SELECT * FROM {}""".format(trainready_table)
    df = hive_context.sql(command)
    generate_tf_statistics(df, tf_statis_path)
    df.write.format("tfrecords").option("recordType", "Example").mode('overwrite').save(tfrecords_hdfs_path)


if __name__ == "__main__":

    sc, hive_context, cfg = load_config(description="generate tf records")
    resolve_placeholder(cfg)
    cfgp = cfg['pipeline']
    trainready_table = cfgp['main_trainready']['trainready_output_table']
    tfrecords_hdfs_path = cfgp['tfrecords']['tfrecords_hdfs_path']
    tf_statis_path = cfgp['tfrecords']['tfrecords_statistics_path']
    # save selected columns of train ready table as tfrecords.
    save_tfrecords(hive_context, trainready_table, tfrecords_hdfs_path, tf_statis_path)
    sc.stop()
