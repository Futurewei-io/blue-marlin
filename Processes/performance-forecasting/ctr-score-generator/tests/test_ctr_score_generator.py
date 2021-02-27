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
from test_base import TestBase
from data import test_set
from ctr_score_generator import ctr_score_generator
from kazoo.client import KazooClient

# Baohua Cao.


class Test_Ctr_Score_Generator(TestBase):

    def test_run(self):
        """
        test run() in ctr_score_generator.py
        """
        # prepare tested gucdocs and keywords.
        df_ucdoc_tested = self.hive_context.createDataFrame(
            test_set.gucdoc_tested, test_set.gucdoc_columns
        )
        df_keyword_tested = self.hive_context.createDataFrame(
            test_set.keyword_tested, test_set.keyword_columns
        )
        # prepare din tf serving url and din model length config from zookeeper.
        cfg_zk = self.cfg["zookeeper"]
        zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
        zk.start()
        din_model_tf_serving_url, stat = zk.get(
            cfg_zk["din_model_tf_serving_url"])
        din_model_tf_serving_url = din_model_tf_serving_url.replace("'", "")
        din_model_length, stat = zk.get(cfg_zk["din_model_length"])
        
        # test the major fun() of a ctr score generator.
        generator = ctr_score_generator.CTR_Score_Generator(
            df_ucdoc_tested, df_keyword_tested, din_model_tf_serving_url, din_model_length)
        generator.run()
        df_ucdocs_loaded = generator.df_gucdocs_loaded
        # check each keyword's ctr score among the loaded gucdocs.
        # each keyword in the keywords test set at least should get a non-zero score.
        df_keywords = df_keyword_tested.select('keyword').distinct()
        # in the test dataset, at least each keyword should have one non-zero predicted score.
        for df_keyword in df_keywords.collect():
            keyword = df_keyword["keyword"]
            keyword_scores_sum = 0
            for df_ucdoc in df_ucdocs_loaded.collect():
                keyword_scores_sum += df_ucdoc[keyword]
            self.assertTrue(keyword_scores_sum > 0)

    def test_load_df_with_zookeeper(self):
        # test loading the config parameters' values from zookeeper.
        cfg_zk = self.cfg["zookeeper"]
        zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
        zk.start()
        _ = ctr_score_generator.load_df_with_zookeeper(cfg_zk, zk)
        df_gucdocs, df_keywords, din_tf_serving_url, length = _
        self.assertTrue(df_gucdocs.count() > 0)
        self.assertTrue(df_keywords.count() > 0)
        self.assertTrue(din_tf_serving_url is not None)
        self.assertTrue(length is not None)


if __name__ == "__main__":
    unittest.main()
