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

import unittest
import datetime
from test_base import TestBase
from data import test_set
from inventory_updater import inventory_updater
from kazoo.client import KazooClient
from pyspark import SparkContext
from pyspark.sql import HiveContext

# Baohua Cao.


class Test_Inventory_Updater(TestBase):

    def test_run(self):
        """
        test run() in inventory_updater.py
        """
        df_gucdoc_tested = self.hive_context.createDataFrame(
            test_set.gucdoc_tested, test_set.gucdoc_columns
        )

        cfg_zk = self.cfg["zookeeper"]
        zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
        zk.start()

        gucdocs_table, stat = zk.get(cfg_zk["gucdocs_loaded_table"])
        keywords_table, stat = zk.get(cfg_zk["keywords_table"])

        sc = SparkContext.getOrCreate()
        sc.setLogLevel('WARN')
        hive_context = HiveContext(sc)

        command = "select distinct keyword from {}"
        df_keywords = hive_context.sql(command.format(keywords_table))
        keywords = [keyword['keyword'] for keyword in df_keywords.collect()]

        es_host, stat = zk.get(cfg_zk["es_host"])
        es_port, stat = zk.get(cfg_zk["es_port"])
        es_index, stat = zk.get(cfg_zk["es_index"])
        es_index = es_index + '_' + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        es_type, stat = zk.get(cfg_zk["es_type"])
        es_starting_day, stat = zk.get(cfg_zk["es_starting_day"])
        es_ending_day, stat = zk.get(cfg_zk["es_ending_day"])
        es_starting_hour, stat = zk.get(cfg_zk["es_starting_hour"])
        es_ending_hour, stat = zk.get(cfg_zk["es_ending_hour"])
        ims_inventory_serving_url_daily, stat = zk.get(
            cfg_zk["ims_inventory_serving_url_daily"])
        es = {"es.nodes": es_host, "es.port": es_port,
          "es.resource": es_index+'/'+es_type,
          "es.batch.size.bytes": "1000000",
          "es.batch.size.entries": "100",
          "es.input.json": "yes",
          "es.mapping.id": "uckey",
          "es.nodes.wan.only": "true",
          "es.write.operation": "upsert",
          'st': es_starting_day, 'ed': es_ending_day,
          'sh': es_starting_hour, 'eh': es_ending_hour}
        updater = inventory_updater.Inventory_Updater(df_gucdoc_tested, keywords, es)
        updater.run()
        self.assertTrue(updater.df_gucdocs_updated is not None)
        self.assertTrue(updater.df_gucdocs_updated.count() == 1)

        zk.stop()
       

if __name__ == "__main__":
    unittest.main()
