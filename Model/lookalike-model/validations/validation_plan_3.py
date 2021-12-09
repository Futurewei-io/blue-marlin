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

import itertools
from pyspark.sql.functions import lit, col, udf, rand, avg
from pyspark.sql import HiveContext
from pyspark import SparkContext
from datetime import datetime, timedelta

from pyspark.sql.types import BooleanType, FloatType


'''
spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict validation_plan_3.py
'''


def to_timestamp(x):
    dt = datetime.strptime(x, '%Y-%m-%d')
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds())


def str_to_map(_str):
    result = {}
    for part in _str.split(','):
        (kw, count) = part.split(':')
        if kw not in result:
            result[kw] = 0
        result[kw] += int(count)
    return result


# TODO: Mocked.
def extend_users(k, seed_users):
    return [_['aid'] for _ in seed_users]


def get_avg_ctr(user_id_list, target_kw, starting_time, ending_time):
    df = hive_context.sql('SELECT * FROM {}'.format(cfg['lookalike_trainready']))
    user_id_set = set(user_id_list)
    df = df.filter(udf(lambda x: x in user_id_set, BooleanType())(df.aid))
    df = df.withColumn('ctr', udf(get_user_ctr_for_kw(target_kw=target_kw, starting_time=starting_time, ending_time=ending_time),
                                  FloatType())(df.interval_starting_time, df.kwi_show_counts, df.kwi_click_counts))
    result = df.agg(avg('ctr')).take(1)[0][0]
    if result:
        return result
    return float(0)


def get_user_ctr_for_kw(target_kw, starting_time, ending_time):
    def _helper(interval_starting_time, kwi_show_counts, kwi_click_counts):
        zip_data = itertools.izip(interval_starting_time, kwi_show_counts, kwi_click_counts)
        shows = 0
        clicks = 0
        for item in zip_data:
            action_time = int(item[0])
            if action_time >= starting_time and action_time <= ending_time:
                show_map = str_to_map(item[1])
                click_map = str_to_map(item[2])
                if target_kw in show_map:
                    shows += show_map[target_kw]
                if target_kw in click_map:
                    clicks += click_map[target_kw]

        ctr = float(0)
        if shows > 0:
            ctr = float(clicks * 1.0 / shows)
        return ctr
    return _helper


def find_n_users_with_interest_in_kw(hive_context, target_kw, seed_size, min_ctr, starting_time, ending_time):

    df = hive_context.sql('SELECT * FROM {}'.format(cfg['lookalike_trainready']))
    df = df.withColumn('ctr', udf(get_user_ctr_for_kw(target_kw=target_kw, starting_time=starting_time, ending_time=ending_time),
                                  FloatType())(df.interval_starting_time, df.kwi_show_counts, df.kwi_click_counts))
    df = df.filter(udf(lambda _ctr: _ctr >= min_ctr, BooleanType())(df.ctr))

    users = df.take(seed_size)
    for user in users:
        print(user)


def run(hive_context, cfg, target_kw):
    starting_time = to_timestamp(cfg['start_date'])
    ending_time = to_timestamp(cfg['end_date'])
    seed_users = find_n_users_with_interest_in_kw(hive_context=hive_context, target_kw=target_kw, seed_size=cfg['seed_size'],
                                                  min_ctr=cfg['min_ctr'], starting_time=starting_time, ending_time=ending_time)

    extended_users = extend_users(cfg['extended_users'], seed_users)

    user_id_list = extended_users
    avg_ctr_1 = get_avg_ctr(user_id_list, starting_time, ending_time)

    random_users = hive_context.sql('SELECT aid FROM {}'.format(cfg['lookalike_trainready'])).take(cfg['extended_users'])
    random_users = [_['aid'] for _ in random_users]
    avg_ctr_2 = get_avg_ctr(random_users, starting_time, ending_time)

    print(avg_ctr_1)
    print(avg_ctr_2)


if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    cfg = {
        'start_date': '2021-06-01',
        'end_date': '2021-07-20',
        'lookalike_trainready': 'lookalike_11192021_trainready',
        'seed_size': 100,
        'min_ctr': 0.7,
        'extended_users': 10000
    }

    target_kw = '10'

    run(hive_context=hive_context, cfg=cfg, target_kw=target_kw)
    sc.stop()
