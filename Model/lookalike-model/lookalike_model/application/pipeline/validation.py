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

from pyspark.sql.functions import lit, col, udf, rand
from pyspark.sql import HiveContext
from pyspark import SparkContext
import argparse, yaml
from util import resolve_placeholder


def counting(click,kwi):
    count_click = 0
    for row in click:
        if len(row) != 0:
            l = row[0]
            count_click += sum([int(j.split(':')[1]) for j in l.split(',') if j.split(':')[0] == kwi])
    return count_click

def run(hive_context,cfg, kwi):
    lookalike_score_table = cfg["output"]["similarity_table"]
    seed_user_table = cfg['input']['seeduser_table']
    extend = cfg['input']['extend']
    test_table = cfg['input']['test_table']
    number_of_seeduser = cfg['input']['number_of_seeduser']

    ######### filtering the df and removing seed users
    command = "select * from {}"
    user = hive_context.sql(command.format(seed_user_table)) #look_alike_seeduser
    user_list = [row[0] for row in user.select('did').collect()]
    command = "select * from {}"
    df = hive_context.sql(command.format(lookalike_score_table)) #lookalike_similarity
    df = df.filter(~col('did').isin(user_list))
    df_test = hive_context.sql(command.format(test_table)) #"lookalike_trainready_jimmy_test"

    #######user_form score
    fdf = df.select('did').orderBy(col('mean_score').desc())
    did_list = [row[0] for row in fdf.select('did').collect()][0:extend]
    count_df = df_test.filter(col('did').isin(did_list))
    click = [row[0] for row in count_df.select('keyword_indexes_click_counts').collect()]
    print("click count is: ",counting(click,kwi))

    ########user form random
    random_user = df.select('did').orderBy(rand())
    did_list = [row[0] for row in random_user.select('did').collect()][0:extend]
    count_df = df_test.filter(col('did').isin(did_list))
    click = [row[0] for row in count_df.select('keyword_indexes_click_counts').collect()]
    print("click count is: ",counting(click,kwi))

if __name__ == "__main__":
    """
    validate the result
    """
    parser = argparse.ArgumentParser(description=" ")
    parser.add_argument('config_file')
    parser.add_argument('kwi')
    args = parser.parse_args()
    kwi = args.kwi
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)
        resolve_placeholder(cfg)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    run(hive_context=hive_context,cfg=cfg, kwi=kwi)
    sc.stop()