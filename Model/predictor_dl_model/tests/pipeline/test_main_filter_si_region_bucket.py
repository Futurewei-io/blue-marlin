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
from predictor_dl_model.pipeline import main_filter_si_region_bucket
from pyspark.sql.types import IntegerType
from test_base import TestBase
from data import test_set

# Baohua Cao optimized.


class TestMainFilterSiRegionBucket(TestBase):

    def test_drop_region(self):
        """
        tests drop_region() in main_filter_si_region_bucket.py
        """
        df_tested = self.hive_context.createDataFrame(
            test_set.factdata_tested,
            test_set.factdata_columns
        )
        df_expected = self.hive_context.createDataFrame(
            test_set.factdata_expected_drop_region,
            test_set.factdata_columns
        )
        columns = test_set.factdata_columns
        df = main_filter_si_region_bucket.drop_region(df_tested)
        self.assertTrue(self.compare_dfs(
            df.select(columns), df_expected.select(columns)))

    def test_modify_ipl(self):
        """
        tests modify_ipl() in main_filter_si_region_bucket.py
        """
        mapping_df = self.hive_context.createDataFrame(
            test_set.region_mapping_tested,
            test_set.region_mapping_columns_renamed
        )
        df_tested = self.hive_context.createDataFrame(
            test_set.factdata_expected_drop_region,
            test_set.factdata_columns
        )
        df_expected = self.hive_context.createDataFrame(
            test_set.factdata_expected_region_mapped,
            test_set.factdata_columns
        )
        columns = test_set.factdata_columns
        df = main_filter_si_region_bucket.modify_ipl(df_tested, mapping_df)
        self.assertTrue(self.compare_dfs(
            df.select(columns), df_expected.select(columns)))

    def test_assign_new_bucket_id(self):
        """
        tests assign_new_bucket_id() in main_filter_si_region_bucket.py
        """
        df_tested = self.hive_context.createDataFrame(
            test_set.factdata_new_bucket_tested,
            test_set.factdata_columns
        )
        df_expected = self.hive_context.createDataFrame(
            test_set.factdata_new_bucket_expected,
            test_set.factdata_columns
        )
        columns = test_set.factdata_columns
        df = main_filter_si_region_bucket.assign_new_bucket_id(
            df_tested, test_set.new_bucket_size)
        df = df.withColumn('bucket_id', df.bucket_id.cast(IntegerType()))
        for dfi in df.collect():
            print(dfi)
        self.assertTrue(self.compare_dfs(
            df.select(columns), df_expected.select(columns)))


if __name__ == "__main__":
    unittest.main()
