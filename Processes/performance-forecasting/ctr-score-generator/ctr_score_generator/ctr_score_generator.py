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
from din_model.trainer.rest_client import predict, str_to_intlist
from kazoo.client import KazooClient


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
            def __helper(uckey, keyword_indexes_show_counts, age, gender):
                keyword_indexes_show_counts = str_to_intlist(keyword_indexes_show_counts)
                record = {'ucdoc': uckey,
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
                                                            (col('uckey_index'), col('keyword_indexes_show_counts'),
                                                             col('age'), col('gender')))


def load_df_with_zookeeper(hive_context, cfg_zk, zk):
    # get config parameters from zookeeper and then load dfs.
    gucdocs_table, stat = zk.get(cfg_zk["gucdocs_table"])
    keywords_table, stat = zk.get(cfg_zk["keywords_table"])

    # load dfs from hive tables then return.
    command = "SELECT * FROM {}"
    df_gucdocs = hive_context.sql(command.format(gucdocs_table))
    df_keywords = hive_context.sql(command.format(keywords_table))
    din_serving_url, stat = zk.get(cfg_zk["din_model_tf_serving_url"])
    din_model_length, stat = zk.get(cfg_zk["din_model_length"])

    return df_gucdocs, df_keywords, din_serving_url, din_model_length


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Performance Forecasting: CTR Score Generator")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    cfg_zk = cfg["zookeeper"]
    zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
    zk.start()

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    # load dataframes for score generator with zookeeper configs on the cluster
    df_gucdocs, df_keywords, din_tf_serving_url, length = load_df_with_zookeeper(hive_context, cfg_zk, zk)
    gucdocs_loaded_table, stat = zk.get(cfg_zk["gucdocs_loaded_table"])

    # create a CTR score generator instance and run to get the loaded gucdocs
    ctr_score_generator = CTRScoreGenerator(df_gucdocs, df_keywords, din_tf_serving_url, length)
    ctr_score_generator.run()
    df_gucdocs_loaded = ctr_score_generator.df_gucdocs_loaded

    media_slot_mapping_table = cfg["mapping"]["media_slot_mapping_table"]
    is_gen_media_slot_mapping_table = cfg["mapping"]["is_gen_media_slot_mapping_table"]
    if is_gen_media_slot_mapping_table:
        df_media_slot = gen_mappings_media(hive_context, cfg)
        df_media_slot.write.option("header", "true").option(
            "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(media_slot_mapping_table)
    else:
        df_media_slot = hive_context.sql("select * from {}".format(media_slot_mapping_table))

    # gucdocs join with media_slot_mapping_table
    df_gucdocs_loaded = df_gucdocs_loaded.join(df_media_slot, on=['media_category'], how='left')

    # save the loaded gucdocs to hive table
    df_gucdocs_loaded.write.option("header", "true").option(
        "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(gucdocs_loaded_table)
