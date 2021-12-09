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
from datetime import datetime, timedelta
import timeit

from pyspark import SparkContext
from pyspark.sql.functions import col, udf, collect_set
from pyspark.sql.types import BooleanType, IntegerType, StringType
from pyspark.sql import HiveContext
from util import load_config, load_batch_config, load_df
from util import write_to_table, write_to_table_with_partition, generate_add_keywords, print_batching_info, resolve_placeholder
import hashlib


'''
inputs are log files and persona

ads_clicklog_11162021
ads_showlog_11162021
ads_persona_11162021


output for logs are like

+----------------------------------------------------------------+--------------------------------+-----------------------+------+---+-------+----------+----------+
|aid                                                             |slot_id                         |action_time            |gender|age|keyword|day       |aid_bucket|
+----------------------------------------------------------------+--------------------------------+-----------------------+------+---+-------+----------+----------+
|006593134ddca7f681fb74da5f3cc3a5b8697df16298cbc5d3a3cb9dacfa0502|w9fmyd5r0i                      |2021-07-15 08:10:42.237|g_m   |5  |1519   |2021-07-15|1         |
|0389986b9fb9105c00aec494b1be54374dddf516543ad3fb6ec7fe3a859223a2|7b0d7b55ab0c11e68b7900163e3e481d|2021-07-15 14:18:56.610|g_m   |2  |1519   |2021-07-15|1         |
|07d1f8749ba8189f24c277e7a34d529076df90918a3550fbe72227c3241951a7|d971z9825e                      |2021-07-15 18:47:34.298|g_f   |2  |3201   |2021-07-15|1         |
|1b694d877035f608b72d3b1c02bd1637934ba89577df5407c765906a4c2f2124|66bcd2720e5011e79bc8fa163e05184e|2021-07-15 18:13:10.173|g_f   |5  |1519   |2021-07-15|1         |
|1ed751cbab9975b80760a2746aab30dfd04bcd41d848fce3ca447224150c0e2b|a47eavw7ex                      |2021-07-15 19:59:56.074|g_m   |5  |1519   |2021-07-15|1         |
+----------------------------------------------------------------+--------------------------------+-----------------------+------+---+-------+----------+----------+


'''


def clean_persona(df, aid_bucket_num):
    """
    This cleans persona table to have distinct aid, gender and age.
    Every aid is associated to only one age and gender
    [Row(aid=u'a1f5e194fa29ecb1a10a8c813731d7b289cf8b2a30cf8b7784cf5e8258e2bb9d',
    gender_new_dev=u'1', forecast_age_dev=u'4')]
    """
    df = df.select('aid', 'gender', 'age').distinct()
    df_aid_count = df.groupBy('aid').agg({'aid': 'count'})
    df = df.join(df_aid_count, on=['aid'], how='left')
    df = df.filter(col('count(aid)') == 1)
    df = df.drop(col('count(aid)'))
    df = df.withColumn("gender", df["gender"].cast(StringType()))
    df = df.withColumn("age", df["age"].cast(IntegerType()))
    df = add_aid_bucket(df, aid_bucket_num)
    return df


def add_day(df):
    # action_time has the format "%Y-%m-%d %H:%M:%S"
    df = df.withColumn('day', udf(lambda x: x[:10], StringType())(col('action_time')))
    return df

def add_gender_index(df):
    df = df.withColumn('gender_index', udf(lambda x: 1 if x=="g_f" else (0 if x=="g_m" else 2), IntegerType())(col('gender')))
    return df

def add_aid_bucket(df, n):
    def __hash_sha256(s):
        hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
        return int(hex_value, 16)
    _udf = udf(lambda x: __hash_sha256(x) % n)
    df = df.withColumn('aid_bucket', _udf(df.aid))
    return df


def clean_batched_log(df, df_persona, df_keywords, aid_bucket_num):
    """
    df: show-log or click-log dataframe
    df_persona: persona dataframe
    df_keywords: keywords-spread-app-id dataframe

    This methods:
    1. Add gender and age from persona table to each record of log
    2. Add keyword to each row by looking to spread-app-id
    """
    def filter_new_si(df, new_slot_id_list):
        """
        This filters logs with pre-defined slot-ids.
        """
        new_si_set = set(new_slot_id_list)
        _udf = udf(lambda x: x in new_si_set, BooleanType())
        df = df.filter(_udf(df.slot_id))
        return df

    # new_slot_id_list = conditions['new_slot_id_list']
    # df = filter_new_si(df, new_slot_id_list)

    df = df.join(df_persona, on=['aid'], how='inner')
    df = df.join(df_keywords, on=['keyword'], how="inner")
    df = add_day(df)
    df = add_aid_bucket(df, aid_bucket_num)
    df = add_gender_index(df)
    return df

def filter_keywords(df, keywords):
    # User defined function to return if the keyword is in the inclusion set.
    _udf = udf(lambda x: x in keywords, BooleanType())

    # Return the filtered dataframe.
    return df.filter(_udf(col('keyword')))

def clean_logs(cfg, df_persona, df_keywords, log_table_names):
    sc = SparkContext.getOrCreate()
    sc.setLogLevel(cfg['log']['level'])
    hive_context = HiveContext(sc)
    cfg_clean = cfg['pipeline']['main_clean']
    # conditions = cfg_clean['conditions']
    start_date, end_date, load_minutes = load_batch_config(cfg)
    aid_bucket_num = cfg_clean['did_bucket_num']

    timer_start = timeit.default_timer()
    showlog_table, showlog_output_table, clicklog_table, clicklog_output_table = log_table_names
    starting_time = datetime.strptime(start_date, "%Y-%m-%d")
    ending_time = datetime.strptime(end_date, "%Y-%m-%d")
    columns = ['aid','slot_id', 'action_time', 'gender','gender_index', 'age', 'keyword', 'keyword_index','day', 'aid_bucket'] ##'keyword_index',

    batched_round = 1
    while starting_time < ending_time:
        time_start = starting_time.strftime("%Y-%m-%d %H:%M:%S")
        batch_time_end = starting_time + timedelta(minutes=load_minutes)
        batch_time_end = min(batch_time_end, ending_time)
        time_end = batch_time_end.strftime("%Y-%m-%d %H:%M:%S")
        print_batching_info("Main clean", batched_round, time_start, time_end)

        # TODO: Add partitions in the query if the log files are partitioned by day.
        command = """SELECT 
                    aid,
                    slot_id, 
                    industry_id as keyword,
                    {time} AS action_time 
                    FROM {table} WHERE {time} >= '{time_start}' AND {time} < '{time_end}' """



        df_showlog_batched = hive_context.sql(command.format(time='event_time', table=showlog_table, time_start=time_start, time_end=time_end))
        df_clicklog_batched = hive_context.sql(command.format(time='event_time', table=clicklog_table, time_start=time_start, time_end=time_end))

        # write_to_table(df_showlog_batched, "ads_showlog_0520_2days", mode='overwrite')
        # write_to_table(df_clicklog_batched, "ads_clicklog_0520_2days", mode='overwrite')
        # return
 
        # Node: for mode='append' spark might throw socket closed exception, it was due to bug in spark and does not affect data and table.
        mode = 'overwrite' if batched_round == 1 else 'append'

        df_showlog_batched = clean_batched_log(df_showlog_batched, df_persona, df_keywords, aid_bucket_num=aid_bucket_num)
        df_showlog_batched = df_showlog_batched.select(columns)
        write_to_table_with_partition(df_showlog_batched, showlog_output_table, partition=('day', 'aid_bucket'), mode=mode)

        df_clicklog_batched = clean_batched_log(df_clicklog_batched, df_persona, df_keywords, aid_bucket_num=aid_bucket_num)
        df_clicklog_batched = df_clicklog_batched.select(columns)
        write_to_table_with_partition(df_clicklog_batched, clicklog_output_table, partition=('day', 'aid_bucket'), mode=mode)

        batched_round += 1
        starting_time = batch_time_end

    timer_end = timeit.default_timer()
    print('Total batching seconds: ' + str(timer_end - timer_start))


def run(hive_context, cfg):
    """
    # This cleans persona, clicklog and showlog tables,
    # by having persona table with distinct (aid,gender,age) and
    # by removing unassociated slot-id and aid in the log tables.
    """
    cfg_clean = cfg['pipeline']['main_clean']

    persona_table = cfg['persona_table_name']
    clicklog_table = cfg['clicklog_table_name']
    showlog_table = cfg['showlog_table_name']
    keywords_table = cfg['keywords_table']
    create_keywords = cfg_clean['create_keywords']

    persona_new_table = cfg_clean['persona_output_table']
    clicklog_new_table = cfg_clean['clicklog_output_table']
    showlog_new_table = cfg_clean['showlog_output_table']

    aid_bucket_num = cfg_clean['did_bucket_num']

    keywords_effective_table = cfg['pipeline']['main_keywords']['keyword_output_table']

    command = """SELECT aid, 
                        gender_new_dev AS gender, 
                        forecast_age_dev AS age 
                        FROM {}""".format(persona_table)
    df_persona = hive_context.sql(command)

    df_persona = clean_persona(df_persona, aid_bucket_num)

    # Use keywords to clean the clicklog and showlog which do not have any keyword association.
    # Create ad keywords table if does not exist, else load the keywords.
    # if create_keywords:
        # df_keywords = generate_add_keywords(keywords_table)
    # else:
    # df_keywords = load_df(hive_context, keywords_table)
    #[Row(keyword=u'education', keyword_index=1, spread_app_id=u'C100203741')]

    # Use the effective keyword table to filter the keyword table which
    # will serve to filter the show and click log tables.
    df_keywords = load_df(hive_context, keywords_effective_table)
    # df_keywords = df_effective_keywords.select(collect_set('keyword')).first()[0]
    # df_keywords = filter_keywords(df_keywords, effective_keywords)

    log_table_names = (showlog_table, showlog_new_table, clicklog_table, clicklog_new_table)

    clean_logs(cfg, df_persona, df_keywords, log_table_names)

    write_to_table_with_partition(df_persona, persona_new_table, partition=('aid_bucket'), mode='overwrite')


if __name__ == "__main__":

    """
    main_clean is a process to generate cleaned persona, clicklog and showlog.
    """
    sc, hive_context, cfg = load_config(
        description="clean data of persona, clicklog and showlog.")
    hive_context.setConf("hive.exec.dynamic.partition", "true")
    hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    resolve_placeholder(cfg)
    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
