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

from pyspark import SparkContext
from pyspark.sql import HiveContext
import yaml
import argparse

'''
input: logfile
output: lookalike_seeduser table


'''


def run(hive_context, cfg, kwi):
    seed_user_table = cfg['input']['seeduser_table']
    log_table = cfg['input']['log_table']
    number_of_seeduser = cfg['input']['number_of_seeduser']

    # command = "select * from (select * from {} where is_click=1 and keyword_index=29) as s join (select * from {} where is_click=1 and keyword_index=26) as b on b.did = s.did where s.gender = 1"
    command = "SELECT * FROM {} WHERE is_click=1 AND keyword_index={}"
    df = hive_context.sql(command.format(log_table, kwi))
    user_list = df.select('did').alias('did').distinct().limit(number_of_seeduser)
    user_list.cache()

    user_list.write.option("header", "true").option(
        "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(seed_user_table)


if __name__ == "__main__":
    """
    select seed users
    """
    parser = argparse.ArgumentParser(description=" ")
    parser.add_argument('config_file')
    parser.add_argument('kwi')
    args = parser.parse_args()
    kwi = args.kwi
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    run(hive_context=hive_context, cfg=cfg, kwi=kwi)
    sc.stop()
