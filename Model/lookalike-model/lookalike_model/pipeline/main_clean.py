# Copyright 2020, Futurewei Technologies
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

import yaml
import argparse
from datetime import datetime, timedelta
import timeit

from pyspark import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, IntegerType, StringType
from pyspark.sql import HiveContext
from util import load_config, load_batch_config, load_df
from util import write_to_table, write_to_table_with_partition, generate_add_keywords, print_batching_info, resolve_placeholder
import hashlib


def clean_persona(df, did_bucket_num):
    """
    This cleans persona table to have distinct did, gender and age.
    Every did is associated to only one age and gender
    [Row(did=u'a1f5e194fa29ecb1a10a8c813731d7b289cf8b2a30cf8b7784cf5e8258e2bb9d', 
    gender_new_dev=u'1', forecast_age_dev=u'4')]
    """
    df = df.select('did', 'gender', 'age').distinct()
    df_did_count = df.groupBy('did').agg({'did': 'count'})
    df = df.join(df_did_count, on=['did'], how='left')
    df = df.filter(col('count(did)') == 1)
    df = df.drop(col('count(did)'))
    df = df.withColumn("gender", df["gender"].cast(IntegerType()))
    df = df.withColumn("age", df["age"].cast(IntegerType()))
    df = add_did_bucket(df, did_bucket_num)
    return df


def add_day(df):
    # action_time has the format "%Y-%m-%d %H:%M:%S"
    df = df.withColumn('day', udf(lambda x: x[:10], StringType())(col('action_time')))
    return df


def add_did_bucket(df, n):
    def __hash_sha256(s):
        hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
        return int(hex_value, 16)
    _udf = udf(lambda x: __hash_sha256(x) % n)
    df = df.withColumn('did_bucket', _udf(df.did))
    return df


def clean_batched_log(df, df_persona, conditions, df_keywords, did_bucket_num):
    """
    df: show-log or click-log dataframe
    df_persona: persona dataframe
    conditions: list of slot-ids and associated media-category
    df_keywords: keywords-spread-app-id dataframe

    This methods:
    1. Filters right slot-ids and add media-category.
    2. Add gender and age from persona table to each record of log
    3. Add keyword to each row by looking to spread-app-id
    """
    def filter_new_si(df, new_slot_id_list, new_slot_id_app_name_list):
        """
        This filters logs with pre-defined slot-ids.
        """
        new_si_set = set(new_slot_id_list)
        _udf = udf(lambda x: x in new_si_set, BooleanType())
        df = df.filter(_udf(df.slot_id))
        slot_map = dict(zip(new_slot_id_list, slot_app_map))
        _udf_map = udf(lambda x: slot_map[x] if x in slot_map else '', StringType())
        df = df.withColumn('media_category', _udf_map(df.slot_id))
        return df

    new_slot_id_list = conditions['new_slot_id_list']
    slot_app_map = conditions['new_slot_id_app_name_list']
    df = filter_new_si(df, new_slot_id_list, slot_app_map)
    df = df.join(df_persona, on=['did'], how='inner')
    df = df.join(df_keywords, on=['spread_app_id'], how="inner")
    df = add_day(df)
    df = add_did_bucket(df, did_bucket_num)
    return df


def clean_logs(cfg, df_persona, df_keywords, log_table_names):
    sc = SparkContext.getOrCreate()
    sc.setLogLevel(cfg['log']['level'])
    hive_context = HiveContext(sc)
    cfg_clean = cfg['pipeline']['main_clean']
    conditions = cfg_clean['conditions']
    start_date, end_date, load_minutes = load_batch_config(cfg)
    did_bucket_num = cfg_clean['did_bucket_num']

    timer_start = timeit.default_timer()
    showlog_table, showlog_output_table, clicklog_table, clicklog_output_table = log_table_names
    starting_time = datetime.strptime(start_date, "%Y-%m-%d")
    ending_time = datetime.strptime(end_date, "%Y-%m-%d")
    columns = ['spread_app_id', 'did', 'adv_id', 'media', 'slot_id', 'device_name', 'net_type', 'price_model',
               'action_time', 'media_category', 'gender', 'age', 'keyword', 'keyword_index', 'day', 'did_bucket']

    batched_round = 1
    while starting_time < ending_time:
        time_start = starting_time.strftime("%Y-%m-%d %H:%M:%S")
        batch_time_end = starting_time + timedelta(minutes=load_minutes)
        batch_time_end = min(batch_time_end, ending_time)
        time_end = batch_time_end.strftime("%Y-%m-%d %H:%M:%S")
        print_batching_info("Main clean", batched_round, time_start, time_end)

        # TODO: Add partitions in the query if the log files are partitioned by day.
        command = """SELECT 
                    did, 
                    adv_id, 
                    adv_type AS media, 
                    slot_id, 
                    spread_app_id, 
                    device_name, 
                    net_type, 
                    adv_bill_mode_cd AS price_model, 
                    {time} AS action_time 
                    FROM {table} WHERE {time} >= '{time_start}' AND {time} < '{time_end}' """

        # command = """SELECT
        #             did,
        #             adv_id,
        #             adv_type,
        #             slot_id,
        #             spread_app_id,
        #             device_name,
        #             net_type,
        #             adv_bill_mode_cd,
        #             {time}
        #             FROM {table} WHERE {time} >= '{time_start}' AND {time} < '{time_end}' """

        df_showlog_batched = hive_context.sql(command.format(time='show_time', table=showlog_table, time_start=time_start, time_end=time_end))
        df_clicklog_batched = hive_context.sql(command.format(time='click_time', table=clicklog_table, time_start=time_start, time_end=time_end))

        # write_to_table(df_showlog_batched, "ads_showlog_0520_2days", mode='overwrite')
        # write_to_table(df_clicklog_batched, "ads_clicklog_0520_2days", mode='overwrite')
        # return

        mode = 'overwrite' if batched_round == 1 else 'append'

        df_showlog_batched = clean_batched_log(df_showlog_batched, df_persona, conditions, df_keywords, did_bucket_num=did_bucket_num)

        # Node: for mode='append' spark might throw socket closed exception, it was due to bug in spark and does not affect data and table.
        df_showlog_batched = df_showlog_batched.select(columns)
        write_to_table_with_partition(df_showlog_batched, showlog_output_table, partition=('day', 'did_bucket'), mode=mode)

        df_clicklog_batched = clean_batched_log(df_clicklog_batched, df_persona, conditions, df_keywords, did_bucket_num=did_bucket_num)
        df_clicklog_batched = df_clicklog_batched.select(columns)
        write_to_table_with_partition(df_clicklog_batched, clicklog_output_table, partition=('day', 'did_bucket'), mode=mode)

        batched_round += 1
        starting_time = batch_time_end

    timer_end = timeit.default_timer()
    print('Total batching seconds: ' + str(timer_end - timer_start))


def run(hive_context, cfg):
    """
    # This cleans persona, clicklog and showlog tables,
    # by having persona table with distinct (did,gender,age) and
    # by removing unassociated slot-id and did in the log tables.
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

    did_bucket_num = cfg_clean['did_bucket_num']

    command = """SELECT did, 
                        gender_new_dev AS gender, 
                        forecast_age_dev AS age 
                        FROM {}""".format(persona_table)
    df_persona = hive_context.sql(command)

    df_persona = clean_persona(df_persona, did_bucket_num)

    # Use keywords to clean the clicklog and showlog which do not have any keyword association.
    # Create ad keywords table if does not exist, else load the keywords.
    if create_keywords:
        df_keywords = generate_add_keywords(keywords_table)
    else:
        df_keywords = load_df(hive_context, keywords_table)
    #[Row(keyword=u'education', keyword_index=1, spread_app_id=u'C100203741')]

    log_table_names = (showlog_table, showlog_new_table, clicklog_table, clicklog_new_table)

    clean_logs(cfg, df_persona, df_keywords, log_table_names)

    write_to_table_with_partition(df_persona, persona_new_table, partition=('did_bucket'), mode='overwrite')


if __name__ == "__main__":

    """
    main_clean is a process to generate cleaned persona, clicklog and showlog.
    """
    sc, hive_context, cfg = load_config(
        description="clean data of persona, clicklog and showlog.")
    resolve_placeholder(cfg)
    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
