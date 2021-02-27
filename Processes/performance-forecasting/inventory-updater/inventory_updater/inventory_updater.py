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
import requests
import datetime
import json
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf, struct, lit, col, array
from pyspark.sql.types import StringType
from kazoo.client import KazooClient


def build_gucdoc_from_concat_string(guckey):
    '''
    ref from din-model main_logs_with_region
    logs = logs.withColumn('uckey', concat_ws(",", col('media')=0,
                               col('media_category')=1, col('net_type')=2,
                               col('gender')=3, col('age')=4, col('region_id')=5))
    '''
    parts = guckey.split(',')
    _gender = {0: "g_m", 1: "g_f", 2: "g_m_g_f"}[int(parts[3])]
    gucdoc = {'uckey': guckey, 'm': parts[0], 't': parts[2], 'g': _gender, 'a': parts[4], 'r': parts[5]}

    return gucdoc


def udf_keyword_transformer(kws_dict):
    # udf for kws in gucdocs.
    kws = dict()
    for kw, v in kws_dict.items():
        kws[kw] = {}
        kws[kw]['v'] = v
        kws[kw]['b'] = int(v * 100) % 100
    return json.dumps(kws)


def udf_traffic_transformer(gucdoc):
    # udf for inventory in gucdocs.
    traffic = dict({
        'm_cat': gucdoc.media_category,
        'si': gucdoc.slot_ids,
        'uckey': gucdoc.uckey
    })
    _gucdoc = build_gucdoc_from_concat_string(gucdoc.uckey)
    traffic = dict(traffic, **_gucdoc)
    return json.dumps(traffic)


def udf_inventory_transformer(uckey, days):
    # udf for traffic in gucdocs.
    """
    request body sample:
    {
        "targetingChannel": {
            "a": [1],
            "g": ["g_m"]
        },
        "days": [
            {
                "ed": "2020-07-10",
                "sh": 1,
                "eh": 23,
                "st": "2020-07-01"
            }
        ]
    }
    return daily result sample:
    {
        "2020-07-01": {
            "h0": 0,
            "h1": 22528490,
            "h2": 1811051,
            "h3": 1525972
        },
        ...
        "2020-07-10": {
            "h0": 0,
            "h1": 22013470,
            "h2": 1103365,
            "h3": 1042275
        }
    }
    """
    body_dict = dict()
    gucdoc = build_gucdoc_from_concat_string(uckey)
    _dict = {}
    for k, v in gucdoc.items():
        if type(v) == list:
            _dict[k] = v
        else:
            _dict[k] = [v]
    body_dict["targetingChannel"] = _dict
    body_dict["days"] = [json.loads(days)]
    body_json = json.loads(json.dumps(body_dict))

    # call IMS Inventory API to get daily inventory values.
    res = requests.post(ims_inventory_serving_url_daily, json=body_json).json()
    inventory = dict()
    if res is None or type(res) != dict:
        raise Exception('Error on ims-service call!')
    if 'code' in res and res['code'] != 200:
        raise Exception(res['exception'])
    if res:
        res_daily_dict = dict(res)
        for day, price_cats in res_daily_dict.items():
            inventory[day] = {}
            total = 0
            for price_cat, price_cat_inv in price_cats.items():
                inventory[day][price_cat] = price_cat_inv
                total += price_cat_inv
            inventory[day]['total'] = total
    return json.dumps(inventory)


def format_data(x, fields):
    # format the df with fields to json.
    _doc = {fields[0]: x[0]}
    for i in range(1, len(x)):
        _doc[fields[i]] = json.loads(x[i])
    return x[0], json.dumps(_doc)


class InventoryUpdater:

    def __init__(self, df_gucdocs, keywords, es):
        self.df_gucdocs = df_gucdocs
        self.keywords = keywords
        self.es = es
        self.df_gucdocs_updated = None
        self.is_saved_to_es = False

    def run(self):
        df = self.df_gucdocs

        days = {'st': self.es['st'], 'ed': self.es['ed'], 'sh': self.es['sh'], 'eh': self.es['eh']}
        days_json_str = json.dumps(days)

        # use 3 udfs to add 3 new columns for further selection.
        udf_keyword = udf(udf_keyword_transformer, StringType())
        udf_traffic = udf(udf_traffic_transformer, StringType())
        udf_inventory = udf(udf_inventory_transformer, StringType())

        df = df.withColumn('kws', udf_keyword(df.kws))
        df = df.withColumn('traffic', udf_traffic(struct([df[x] for x in df.columns])))
        df = df.withColumn('inventory', udf_inventory(df.uckey, lit(days_json_str)))

        self.df_gucdocs_updated = df.select(['uckey', 'kws', 'traffic', 'inventory'])
        self.save_to_es()

    def save_to_es(self):
        if self.df_gucdocs_updated:
            try:
                rdd = self.df_gucdocs_updated.rdd.map(
                    lambda x: format_data(x, ['uckey', 'kws', 'traffic', 'inventory']))
                rdd.saveAsNewAPIHadoopFile(
                    path='-',
                    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                    keyClass="org.apache.hadoop.io.NullWritable",
                    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                    conf=self.es)
                print('Finished saving ' + str(self.df_gucdocs_updated.count()) +
                      ' gucdocs to elastic search with inventory updated.')
                self.is_saved_to_es = True
            except:
                self.is_saved_to_es = False
                raise
        else:
            print('There is no updated gucdocs to be saved to es.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Performance Forecasting: Inventory Updater")
    parser.add_argument('config_file')
    args = parser.parse_args()
    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    # load zookeeper paths and start kazooClient to load configs.
    cfg_zk = cfg["zookeeper"]
    zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
    zk.start()

    # load gucdocs and keywords from the hive.
    gucdocs_table, stat = zk.get(cfg_zk["gucdocs_loaded_table"])
    keywords_table, stat = zk.get(cfg_zk["keywords_table"])

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    command = "SELECT * FROM {}"
    df_gucdocs = hive_context.sql(command.format(gucdocs_table))

    command = "SELECT DISTINCT keyword FROM {}"
    df_keywords = hive_context.sql(command.format(keywords_table))
    keywords = [keyword['keyword'] for keyword in df_keywords.collect()]

    # load all the es configs for saving gucdocs to es.
    es_host, stat = zk.get(cfg_zk["es_host"])
    es_port, stat = zk.get(cfg_zk["es_port"])
    es_index_prefix, stat = zk.get(cfg_zk["es_index_prefix"])
    es_index = es_index_prefix + '-' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    es_type, stat = zk.get(cfg_zk["es_type"])
    es_starting_day, stat = zk.get(cfg_zk["es_starting_day"])
    es_ending_day, stat = zk.get(cfg_zk["es_ending_day"])
    es_starting_hour, stat = zk.get(cfg_zk["es_starting_hour"])
    es_ending_hour, stat = zk.get(cfg_zk["es_ending_hour"])
    ims_inventory_serving_url_daily, stat = zk.get(
        cfg_zk["ims_inventory_serving_url_daily"])

    es = {"es.nodes": es_host, "es.port": es_port,
          "es.resource": es_index + '/' + es_type,
          "es.batch.size.bytes": "1000000",
          "es.batch.size.entries": "100",
          "es.input.json": "yes",
          "es.mapping.id": "uckey",
          "es.nodes.wan.only": "true",
          "es.write.operation": "upsert",
          'st': es_starting_day, 'ed': es_ending_day,
          'sh': es_starting_hour, 'eh': es_ending_hour}

    # use inventory updater and run it to update the gucdocs and save to es.
    inventory_updater = InventoryUpdater(df_gucdocs, keywords, es)
    inventory_updater.run()

    # update the es_index_updated.
    if inventory_updater.is_saved_to_es:
        zk.set(cfg_zk["es_index"], es_index)
    zk.stop()
