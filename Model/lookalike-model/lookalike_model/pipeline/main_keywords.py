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

from datetime import datetime, timedelta
from pyspark import SparkContext

from util import load_config, load_batch_config, print_batching_info
from util import write_to_table, generate_add_keywords, resolve_placeholder
from pyspark.sql.functions import udf, col, monotonically_increasing_id, row_number



def run(hive_context, showlog_table, keywords_mapping_table, create_keywords_mapping,
        start_date, end_date, load_minutes, keyword_threshold, effective_keywords_table):
    """
    # This script goes through the showlog and identifies all the 
    # keywords that comprise a portion of the overall traffic greater
    # than the specified threshold.
    """

    # Create ad keywords table if does not exist.
    # if create_keywords_mapping:
    #     generate_add_keywords(keywords_mapping_table)
    # [Row(keyword=u'education', keyword_index=1, spread_app_id=u'C100203741')]

    starting_time = datetime.strptime(start_date, "%Y-%m-%d")
    ending_time = datetime.strptime(end_date, "%Y-%m-%d")

    # In batches, get the show counts for all of the keywords.
    keyword_totals = {}
    batched_round = 1
    while starting_time < ending_time:
        time_start = starting_time.strftime("%Y-%m-%d %H:%M:%S")
        batch_time_end = starting_time + timedelta(minutes=load_minutes)
        batch_time_end = min(batch_time_end, ending_time)
        time_end = batch_time_end.strftime("%Y-%m-%d %H:%M:%S")
        print_batching_info("Main keywords", batched_round, time_start, time_end)

        # Get the impressions for the time window joined with the keywords.
        command = """SELECT 
                    industry_id as keyword, 
                    event_time
                    FROM {log_table}
                    WHERE event_time >= '{time_start}' AND event_time < '{time_end}' 
                    AND Length(industry_id) != 0 """
        df_showlog_batched = hive_context.sql(command.format(log_table=showlog_table,
                                                             time_start=time_start, time_end=time_end))

        # Get the number of impressions for each keyword.
        df = df_showlog_batched.groupby('keyword').count().collect()

        # Add the impression count for each keyword to the dictionary.
        for row in df:
            keyword_totals[row['keyword']] = keyword_totals.get(row['keyword'], 0) + int(row['count'])
        starting_time = batch_time_end
        batched_round += 1

    # With the total keyword counts calculated, identify the keywords that meet
    # the threshold to be included.
    # Get the total and calculate the count threshold for effective keywords.
    total_impressions = sum(keyword_totals.values())
    impression_threshold = keyword_threshold * total_impressions

    # For each keyword, if its count is greater than the threshold, add
    # it to the effective keyword list.
    effective_keywords = []
    keyword_index = 0
    for key, value in keyword_totals.items():
        if value > impression_threshold:
            effective_keywords.append((key,keyword_index))  # Append as a tuple
            keyword_index += 1

    # Create the dataframe with the results and save to Hive.
    sc = SparkContext.getOrCreate()
    df_effective_keywords = sc.parallelize(effective_keywords).toDF(['keyword', 'keyword_index'])
    write_to_table(df_effective_keywords, effective_keywords_table)


if __name__ == "__main__":
    """
    main_keywords is a process to identify the effective keywords that 
    comprise a percentage of the traffic above a given threshold.
    """
    sc, hive_context, cfg = load_config(
        description="clean data of persona, clicklog and showlog.")
    resolve_placeholder(cfg)

    cfg_clean = cfg['pipeline']['main_clean']
    showlog_table = cfg['showlog_table_name']
    keywords_mapping_table = cfg['keywords_table']
    create_keywords_mapping = cfg_clean['create_keywords']

    cfg_keywords = cfg['pipeline']['main_keywords']
    keyword_threshold = cfg_keywords['keyword_threshold']
    effective_keywords_table = cfg_keywords['keyword_output_table']

    start_date, end_date, load_minutes = load_batch_config(cfg)

    run(hive_context, showlog_table, keywords_mapping_table, create_keywords_mapping,
        start_date, end_date, load_minutes, keyword_threshold, effective_keywords_table)

    sc.stop()
