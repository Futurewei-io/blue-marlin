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
import json
import re
from datetime import datetime


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

def table_exists(table_name, hive_context):
    command = """
            SHOW TABLES LIKE '{}'
            """.format(table_name)

    df = hive_context.sql(command)
    return df.count() > 0

def __save_as_table(df, table_name, hive_context):
    if not table_exists(table_name, hive_context):
        df.write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('overwrite').saveAsTable(table_name)
    else:
        df.write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)


def run(cfg, hive_context):

    log_level = cfg['log_level']
    product_tag = cfg['product_tag']
    pipeline_tag = cfg['pipeline_tag']

    area_map_table = cfg['area_map_table']
    distribution_table = cfg['distribution_table']
    norm_table = cfg['norm_table']
    model_stat_table = cfg['model_stat_table']
    bucket_size = cfg['bucket_size']
    bucket_step = cfg['bucket_step']

    config_table = cfg['config_table']

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
    df = hive_context.sql(command.format(area_map_table))
    print('Filterted Factdata table schema')
    df.printSchema()

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(distribution_table))
    print('Distribution schema')
    df.printSchema()

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(norm_table))
    print('Norm schema')
    df.printSchema()

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(model_stat_table))
    print('Model stat schema')
    df.printSchema()

    # Save the configuration to Hive.
    cfg_json = json.dumps(cfg)
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    df = hive_context.createDataFrame([(now_str, cfg_json)], ['Date', 'Config'])
    __save_as_table(df, config_table, hive_context)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="DLPredictor")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)
        resolve_placeholder(cfg)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    run(cfg=cfg, hive_context= hive_context)