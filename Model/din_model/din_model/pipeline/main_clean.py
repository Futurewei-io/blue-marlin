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
from util import drop_table, create_log_table, write_to_table, load_df, generate_add_keywords


def clean_persona(df):
    """
    This cleans persona table to have distinct did, gender and age.
    Every did is associated to only one age and gender
    [Row(did=u'a1f5e194fa29ecb1a10a8c813731d7b289cf8b2a30cf8b7784cf5e8258e2bb9d', gender_new_dev=u'1', forecast_age_dev=u'4')]
    """
    df = df.select('did', 'gender', 'age').distinct()
    df_did_count = df.groupBy('did').agg({'did': 'count'})
    df = df.join(df_did_count, on=['did'], how='left')
    df = df.filter(col('count(did)') == 1)
    df = df.drop(col('count(did)'))
    df = df.withColumn("gender", df["gender"].cast(IntegerType()))
    df = df.withColumn("age", df["age"].cast(IntegerType()))
    return df


def clean_log(df, df_persona, conditions, df_keywords):

    def filter_new_si(df, new_slot_id_list, new_slot_id_app_name_list):
        """
        This filters logs with pre-defined slot-ids.
        """
        new_si_set = set(new_slot_id_list)
        _udf = udf(lambda x: x in new_si_set, BooleanType())
        df = df.filter(_udf(df.slot_id))
        new_slot_id_app_name = dict(zip(new_slot_id_list, new_slot_id_app_name_list))
        _udf_map = udf(lambda x: new_slot_id_app_name[x] if x in new_slot_id_app_name else '', StringType())
        df = df.withColumn('media_category', _udf_map(df.slot_id))
        return df

    new_slot_id_list = conditions['new_slot_id_list']
    new_slot_id_app_name_list = conditions['new_slot_id_app_name_list']
    df = filter_new_si(df, new_slot_id_list, new_slot_id_app_name_list)
    df = df.join(df_persona, on=['did'], how='inner')
    df = df.join(df_keywords, on=['spread_app_id'], how="inner")
    return df


def load_and_clean_batched_logs(hive_context, starting_date, ending_date, load_logs_in_minutes, df_persona, df_keywords, conditions, log_table_names):
    timer_start = timeit.default_timer()
    batched_round = 1
    showlog_table, showlog_output_table, clicklog_table, clicklog_output_table = log_table_names
    starting_time = datetime.strptime(starting_date, "%Y-%m-%d")
    ending_time = datetime.strptime(ending_date, "%Y-%m-%d")

    while starting_time < ending_time:
        batched_time_start_str = starting_time.strftime("%Y-%m-%d %H:%M:%S")
        batched_time_end = starting_time + timedelta(minutes=load_logs_in_minutes)
        batched_time_end_str = batched_time_end.strftime("%Y-%m-%d %H:%M:%S")
        print('Batch ' + str(batched_round) + ': processing batched logs in time range ' +
              batched_time_start_str + ' - ' + batched_time_end_str)

        command = """select did, adv_id, adv_type as media, slot_id, spread_app_id, device_name, net_type, 
        adv_bill_mode_cd as price_model, {time} as action_time from {table} where {time} >= '{time_start}' and {time} < '{time_end}'"""

        df_clicklog_batched = hive_context.sql(command.format(time='click_time', table=clicklog_table, time_start=batched_time_start_str, time_end=batched_time_end_str))

        df_showlog_batched = hive_context.sql(command.format(time='show_time', table=showlog_table, time_start=batched_time_start_str, time_end=batched_time_end_str))

        mode = 'overwrite' if batched_round == 1 else 'append'
        if not df_showlog_batched.rdd.isEmpty():
            df_showlog_batched = clean_log(df_showlog_batched, df_persona, conditions, df_keywords)
            write_to_table(df_showlog_batched, showlog_output_table, mode=mode)

        if not df_clicklog_batched.rdd.isEmpty():
            df_clicklog_batched = clean_log(df_clicklog_batched, df_persona, conditions, df_keywords)
            write_to_table(df_clicklog_batched, clicklog_output_table, mode=mode)

        batched_round += 1
        starting_time = batched_time_end

    timer_end = timeit.default_timer()
    print('Total running time for processing batched persona, click and show logs in seconds: ' +
          str(timer_end - timer_start))

    return


def run(hive_context, cfg):
    """
    # This cleans persona, clicklog and showlog tables,
    # by having persona table with distinct (did,gender,age) and
    # by removing unassociated slot-id and did in the log tables.
    # This method loads all the required parameters from the config.yml for main_clean.
    """
    cfg_main_clean = cfg['pipeline']['main_clean']
    persona_table = cfg_main_clean['data_input']['persona_table_name']
    clicklog_table = cfg_main_clean['data_input']['clicklog_table_name']
    showlog_table = cfg_main_clean['data_input']['showlog_table_name']
    load_logs_in_minutes = cfg_main_clean['data_input']['load_logs_in_minutes']
    persona_output_table = cfg_main_clean['data_output']['persona_output_table_name']
    clicklog_output_table = cfg_main_clean['data_output']['clicklog_output_table_name']
    showlog_output_table = cfg_main_clean['data_output']['showlog_output_table_name']
    keywords_table = cfg_main_clean['data_input']['ad_keywords_table_name']
    create_keywords_table = cfg_main_clean['data_input']['create_ad_keywords_table']
    conditions = cfg_main_clean['conditions']

    command = 'select did, gender_new_dev as gender, forecast_age_dev as age from {}'.format(persona_table)
    df_persona = hive_context.sql(command)

    df_persona = clean_persona(df_persona)

    # Use ad keywords to clean the clicklog and showlog which do not have a keyword association.
    # Create ad keywords table if does not exist, else load the keywords.
    if create_keywords_table:
        df_keywords = generate_add_keywords(keywords_table)
    else:
        df_keywords = load_df(hive_context, keywords_table)
    #[Row(keyword=u'education', keyword_index=1, spread_app_id=u'C100203741')]

    starting_date, ending_date = conditions['starting_date'], conditions['ending_date']

    log_table_names = (showlog_table, showlog_output_table, clicklog_table, clicklog_output_table)

    load_and_clean_batched_logs(hive_context, starting_date, ending_date, load_logs_in_minutes, df_persona, df_keywords, conditions, log_table_names)

    write_to_table(df_persona, persona_output_table, mode='overwrite')


if __name__ == "__main__":

    """
    main_clean is a process to generate cleaned tables: persona, clicklog and showlog.
    """

    parser = argparse.ArgumentParser(description='clean data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log']['level'])

    run(hive_context=hive_context, cfg=cfg)

    sc.stop()
