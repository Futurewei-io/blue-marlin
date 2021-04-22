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

def run(cfg, hive_context):

    log_level = cfg['log_level']
    product_tag = cfg['product_tag']
    pipeline_tag = cfg['pipeline_tag']

    factdata = cfg['factdata_table']
    distribution_table = cfg['distribution_table']
    norm_table = cfg['norm_table']
    model_stat_table = cfg['model_stat_table']
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']

    es_host = cfg['es_host']
    es_port = cfg['es_port']
    es_predictions_index = cfg['es_predictions_index']
    es_predictions_type = cfg['es_predictions_type']
    holiday_list = cfg['holiday_list']
    traffic_dist = cfg['traffic_dist']

    for key in cfg:
        print('{}:  {}'.format(key, cfg[key]))
    print('')

    print('Output index:')
    print(es_predictions_index.format(product_tag=product_tag, pipeline_tag=pipeline_tag))
    print('')

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(factdata))
    df_factdata_schema = df.schema
    print('Factdata schema')
    df.printSchema()

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(distribution_table))
    df_distribution_schema = df.schema
    print('Distribution schema')
    df.printSchema()

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(norm_table))
    df_norm_schema = df.schema
    print('Norm schema')
    df.printSchema()

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(model_stat_table))
    df_model_schema = df.schema
    print('Model stat schema')
    df.printSchema()



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="DLPredictor")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)


    run(cfg=cfg, hive_context= hive_context)