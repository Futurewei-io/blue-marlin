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


def run(hive_context, cfg):
    seed_user_table = cfg['input']['seeduser_table']
    log_table = cfg['input']['log_table']
    command = "select * from (select * from {} where is_click=1 and keyword_index=29) as s join (select * from {} where is_click=1 and keyword_index=26) as b on b.did = s.did where s.gender = 1"
    df = hive_context.sql(command.format(log_table, log_table))
    user_list = df.select('s.did').alias('did').distinct().limit(400)
    print('number of seed user is: ', user_list.count())

    user_list.write.option("header", "true").option(
        "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(seed_user_table)


if __name__ == "__main__":
    """
    select seed users
    """
    parser = argparse.ArgumentParser(description=" ")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
