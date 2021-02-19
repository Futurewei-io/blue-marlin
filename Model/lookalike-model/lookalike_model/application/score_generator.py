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
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, MapType
# from rest_client import predict, str_to_intlist
import requests
import json
import argparse
# from kazoo.client import KazooClient
from math import sqrt


def flatten(lst):
    f = [y for x in lst for y in x]
    return f


def str_to_intlist(table):
    ji = []
    for k in [table[j].split(",") for j in range(len(table))]:
        s = []
        for a in k:
            b = int(a.split(":")[0])
            s.append(b)
        ji.append(s)
    return ji


def inputData(record, keyword, length):
    if len(record['show_counts']) >= length:
        hist = flatten(record['show_counts'][:length])
        instance = {'hist_i': hist, 'u': record['did'], 'i': keyword, 'j': keyword, 'sl': len(hist)}
    else:
        hist = flatten(record['show_counts'])
        # [hist.extend([0]) for i in range(length - len(hist))]
        instance = {'hist_i': hist, 'u': record['did'], 'i': keyword, 'j': keyword, 'sl': len(hist)}
    return instance


def predict(serving_url, record, length, new_keyword):
    body = {"instances": []}
    for keyword in new_keyword:
        instance = inputData(record, keyword, length)
        body['instances'].append(instance)
    body_json = json.dumps(body)
    result = requests.post(serving_url, data=body_json).json()
    if 'error' in result.keys():
        predictions = result['error']
    else:
        predictions = result['predictions']
    return predictions


def gen_mappings_media(hive_context, cfg):
    # this function generates mappings between the media category and the slots.
    media_category_list = cfg["mapping"]["new_slot_id_media_category_list"]
    media_category_set = set(media_category_list)
    slot_id_list = cfg["mapping"]["new_slot_id_list"]
    # 1 vs 1: slot_id : media_category
    media_slot_mapping = dict()
    for media_category in media_category_set:
        media_slot_mapping[media_category] = []
        for i in range(len(slot_id_list)):
            if media_category_list[i] == media_category:
                media_slot_mapping[media_category].append(slot_id_list[i])
    media_slot_mapping_rows = []
    for media_category in media_category_set:
        media_slot_mapping_rows.append((media_category, media_slot_mapping[media_category]))
    schema = StructType([StructField('media_category', StringType(), True),
                         StructField('slot_ids', ArrayType(StringType()), True)])
    df = hive_context.createDataFrame(media_slot_mapping_rows, schema)
    return df


def normalize(x):
    c = 0
    for key, value in x.items():
        c += (value ** 2)
    C = sqrt(c)
    result = {}
    for keyword, value in x.items():
        result[keyword] = value / C
    return result


udf_normalize = udf(normalize, MapType(StringType(), FloatType()))


class CTRScoreGenerator:
    def __init__(self, df_gucdocs, df_keywords, din_model_tf_serving_url, din_model_length):
        self.df_gucdocs = df_gucdocs
        self.df_keywords = df_keywords
        self.din_model_tf_serving_url = din_model_tf_serving_url
        self.din_model_length = din_model_length
        self.df_gucdocs_loaded = None
        self.keyword_index_list, self.keyword_list = self.get_keywords()

    def get_keywords(self):
        keyword_index_list, keyword_list = list(), list()
        for dfk in self.df_keywords.collect():
            if not dfk["keyword_index"] in keyword_index_list:
                keyword_index_list.append(dfk["keyword_index"])
                keyword_list.append(dfk["keyword"])
        return keyword_index_list, keyword_list

    def run(self):

        def predict_udf(din_model_length, din_model_tf_serving_url, keyword_index_list, keyword_list):
            def __helper(did, keyword_indexes_show_counts, age, gender):
                keyword_indexes_show_counts = str_to_intlist(keyword_indexes_show_counts)
                record = {'did': did,
                          'show_counts': keyword_indexes_show_counts,
                          'a': str(age),
                          'g': str(gender)}

                response = predict(serving_url=din_model_tf_serving_url, record=record,
                                   length=din_model_length, new_keyword=keyword_index_list)

                gucdoc_kw_scores = dict()
                for i in range(len(response)):
                    keyword = keyword_list[i]
                    keyword_score = response[i][0]
                    gucdoc_kw_scores[keyword] = keyword_score

                return gucdoc_kw_scores

            return __helper

        self.df_gucdocs_loaded = self.df_gucdocs.withColumn('kws',
                                                            udf(predict_udf(din_model_length=self.din_model_length,
                                                                            din_model_tf_serving_url=self.din_model_tf_serving_url,
                                                                            keyword_index_list=self.keyword_index_list,
                                                                            keyword_list=self.keyword_list),
                                                                MapType(StringType(), FloatType()))
                                                            (col('did_index'), col('keyword_indexes_show_counts'),
                                                             col('age'), col('gender')))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Performance Forecasting: CTR Score Generator")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    # load dataframes
    gucdocs_table, keywords_table, din_tf_serving_url, length = cfg["input"]["gucdocs_table"], cfg["input"][
        "keywords_table"], cfg["input"]["din_model_tf_serving_url"], cfg["input"]["din_model_length"]

    command = "SELECT * FROM {}"
    df_gucdocs = hive_context.sql(command.format(gucdocs_table))
    df_keywords = hive_context.sql(command.format(keywords_table))
    # temporary adding to filter based on active keywords
    df_keywords = df_keywords.filter((df_keywords.keyword == "video") | (df_keywords.keyword == "shopping") | (df_keywords.keyword == "info") |
                                     (df_keywords.keyword == "social") | (df_keywords.keyword == "reading") | (df_keywords.keyword == "travel") |
                                     (df_keywords.keyword == "entertainment"))
    gucdocs_loaded_table = cfg['output']['gucdocs_loaded_table']
    gucdocs_loaded_table_norm = cfg['output']['gucdocs_loaded_table_norm']

    # create a CTR score generator instance and run to get the loaded gucdocs
    ctr_score_generator = CTRScoreGenerator(df_gucdocs, df_keywords, din_tf_serving_url, length)
    ctr_score_generator.run()
    df_gucdocs_loaded = ctr_score_generator.df_gucdocs_loaded

    df_gucdocs_loaded_norm = df_gucdocs_loaded.withColumn('kws_norm', udf_normalize(col('kws')))

    # save the loaded gucdocs to hive table
    df_gucdocs_loaded.write.option("header", "true").option(
        "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(gucdocs_loaded_table)

    df_gucdocs_loaded_norm.write.option("header", "true").option(
        "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(gucdocs_loaded_table_norm)
