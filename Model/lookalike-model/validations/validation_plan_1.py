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
import argparse
import yaml
from util import resolve_placeholder

'''

spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict validation_plan_1.py config.yml '29'

1. Randomly select 1000 users. It is user_list.
2. Calculate click/imp for kw. It is actual_interest.
3. Sort user_list based on actual_interest and get first 500. This is list M.
4. Sort user_list based on kw score and get first 500. It is list N.
5. Final result is (size of common(M,N) ) / (size of M)


'''


def process(user, kwi):
    l = user['kwi_show_counts']
    imp_for_kwi = sum([int(j.split(':')[1]) if j.split(':')[0] == kwi else 0 for _ in l for j in _.split(',')])

    l = user['kwi_click_counts']
    click_for_kwi = sum([int(j.split(':')[1]) if j.split(':')[0] == kwi else 0 for _ in l for j in _.split(',')])

    actual_interest = click_for_kwi * 1.0 / imp_for_kwi if imp_for_kwi != 0 else 0

    kwi_score = 0
    if kwi in user['kws']:
        kwi_score = user['kws']['kwi']

    return (user['did'], actual_interest, kwi_score)


def run(hive_context, cfg, kwi):
    RANDOM_USERS_SIZE = 1000
    lookalike_score_table = cfg['score_generator']['output']['score_table']

    # Randomly select 1000 users. It is user_list.
    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(lookalike_score_table))
    user_list = df.orderBy(rand()).limit(RANDOM_USERS_SIZE).collect()
    user_metrics = [process(user, kwi) for user in user_list]

    n = sorted(user_metrics, key=lambda x: x[1], reverse=True)[:RANDOM_USERS_SIZE//2]
    m = sorted(user_metrics, key=lambda x: x[2], reverse=True)[:RANDOM_USERS_SIZE//2]

    n = set([_[0] for _ in n])
    m = set([_[0] for _ in m])

    size_of_common = len(n.intersection(m))
    result = size_of_common*1.0/(RANDOM_USERS_SIZE//2)

    print(result)


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

    run(hive_context=hive_context, cfg=cfg, kwi=kwi)
    sc.stop()
