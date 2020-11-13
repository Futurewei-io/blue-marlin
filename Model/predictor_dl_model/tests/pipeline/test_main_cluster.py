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
from predictor_dl_model.pipeline import main_cluster
from test_base import TestBase
from data import test_set
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, FloatType, BooleanType
from predictor_dl_model.pipeline import transform as transform

# Baohua Cao.


class TestMainCluster(TestBase):

    def test_remove_weak_uckeys(self):
        df = self.hive_context.createDataFrame(
            test_set.factdata_main_ts_expected,
            test_set.factdata_main_ts_columns
        )
        # add imp
        df = df.withColumn('imp', udf(lambda ts: sum(
            [_ for _ in ts if _]), IntegerType())(df.ts))
        # add popularity = mean
        df = df.withColumn('p', udf(lambda ts: sum(
            [_ for _ in ts if _])/(1.0*len(ts)), FloatType())(df.ts))
        # remove weak uckeys
        df = main_cluster.remove_weak_uckeys(df, test_set.popularity_th, test_set.datapoints_min_th)
        df_remove_weak_uckeys_expected = self.hive_context.createDataFrame(
            test_set.factdata_remove_weak_uckeys_expected,
            test_set.factdata_remove_weak_uckeys_columns
        )
        columns = test_set.factdata_remove_weak_uckeys_columns
        self.assertTrue(self.compare_dfs(
            df.select(columns), df_remove_weak_uckeys_expected.select(columns)))

    def test_is_spare(self):
        # add normalized popularity = mean_n
        df = self.hive_context.createDataFrame(
            test_set.factdata_remove_weak_uckeys_expected,
            test_set.factdata_remove_weak_uckeys_columns
        )
        df, _ = transform.normalize_ohe_feature(df, ohe_feature='p')

        df = df.withColumn('sparse', udf(
            main_cluster.is_spare(test_set.datapoints_th_uckeys, test_set.popularity_norm), BooleanType())(df.p_n, df.ts))
        
        df_is_spare_uckeys_expected = self.hive_context.createDataFrame(
            test_set.factdata_is_spare_uckeys_expected,
            test_set.factdata_is_spare_uckeys_columns
        )
        columns = test_set.factdata_is_spare_uckeys_columns
        
        self.assertTrue(self.compare_dfs(
            df.select(columns), df_is_spare_uckeys_expected.select(columns)))

    def test_estimate_number_of_non_dense_clusters(self):
        df = self.hive_context.createDataFrame(
            test_set.factdata_is_spare_uckeys_expected,
            test_set.factdata_is_spare_uckeys_columns
        )
        number_of_virtual_clusters = main_cluster.estimate_number_of_non_dense_clusters(
            df, test_set.median_popularity_of_dense)
        self.assertTrue(number_of_virtual_clusters == 2)

    def test_list_to_map(self):
        mlist = [1,2,1,2,1]
        mapped_list = main_cluster.list_to_map(mlist)
        expected_list = {1: 0.6, 2: 0.4}
        self.assertTrue(mapped_list, expected_list)

        mlist = [0, 1, 2]
        mapped_list = main_cluster.list_to_map(mlist)
        expected_list = {0: 0.3333333333333333, 1: 0.3333333333333333, 2: 0.3333333333333333}
        self.assertTrue(mapped_list == expected_list)

        mlist = [0,0,0]
        mapped_list = main_cluster.list_to_map(mlist)
        expected_list = {0: 1.0}
        self.assertTrue(mapped_list == expected_list)

    def test_agg_ts(self):
        mlist = [[1,2,1,2,1],[0,1,2], [0,0,0]]
        agged_list = main_cluster.agg_ts(mlist)
        expected_list = [1, 3, 3, 2, 1]
        self.assertTrue(agged_list, expected_list)


if __name__ == "__main__":
    unittest.main()
