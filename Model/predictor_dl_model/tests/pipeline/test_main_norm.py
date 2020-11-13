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

import unittest
from predictor_dl_model.pipeline import main_norm
from test_base import TestBase
from data import test_set

# Baohua Cao.


class TestMainNorm(TestBase):

    def test_add_ohe_feature(self):
        """
        test add_ohe_feature() in main_norm.py
        """
      
        df_tested = self.hive_context.createDataFrame(
            test_set.factdata_cluster_tested, test_set.factdata_cluster_columns)
      
        df_expected_ohe_a = self.hive_context.createDataFrame(
            test_set.factdata_cluster_expected_ohe_a,
            test_set.factdata_cluster_columns_ohe_a
        )
        feature_name, feature_value_list = 'a', test_set.a_feature_value_list
        df_ohe_a = main_norm.add_ohe_feature(df_tested, feature_name, feature_value_list)
        columns = test_set.factdata_cluster_columns_ohe_a
        self.assertTrue(self.compare_dfs(
            df_ohe_a.select(columns), df_expected_ohe_a.select(columns)))
        
        df_expected_ohe_g = self.hive_context.createDataFrame(
            test_set.factdata_cluster_expected_ohe_g,
            test_set.factdata_cluster_columns_ohe_g
        )
        feature_name, feature_value_list = 'g', test_set.g_feature_value_list
        df_ohe_g = main_norm.add_ohe_feature(df_tested, feature_name, feature_value_list)
        columns = test_set.factdata_cluster_columns_ohe_g
        self.assertTrue(self.compare_dfs(
            df_ohe_g.select(columns), df_expected_ohe_g.select(columns)))

        df_expected_ohe_t = self.hive_context.createDataFrame(
            test_set.factdata_cluster_expected_ohe_t,
            test_set.factdata_cluster_columns_ohe_t
        )
        feature_name, feature_value_list = 't', test_set.t_feature_value_list
        df_ohe_t = main_norm.add_ohe_feature(df_tested, feature_name, feature_value_list)
        columns = test_set.factdata_cluster_columns_ohe_t
        self.assertTrue(self.compare_dfs(
            df_ohe_t.select(columns), df_expected_ohe_t.select(columns)))

    def test_normalize(self):
        mlist = [1,2,3,4,5]
        normalized_list = main_norm.normalize(mlist)
        expected_list = ([-1.2649110640673518, -0.6324555320336759, 0.0, 0.6324555320336759, 1.2649110640673518], 3.0, 1.5811388300841898)
        self.assertTrue(normalized_list == expected_list)

        mlist = [0, 1]
        normalized_list = main_norm.normalize(mlist)
        expected_list = ([-0.7071067811865475, 0.7071067811865475], 0.5, 0.7071067811865476)
        self.assertTrue(normalized_list == expected_list)

        mlist = [0,1,2]
        normalized_list = main_norm.normalize(mlist)
        expected_list = ([-1.0, 0.0, 1.0], 1.0, 1.0)
        self.assertTrue(normalized_list == expected_list)


if __name__ == "__main__":
    unittest.main()
