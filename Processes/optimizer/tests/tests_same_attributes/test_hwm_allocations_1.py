# Copyright 2019, Futurewei Technologies
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

# Testing resource allocation with inventory amount based on b1, b2, b3 with idnex of bookings_03242020.
# Baohua Cao

# Testing hwm_allocation() with natural bookings.

import unittest
from imscommon.es.ims_esclient import ESClient
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType, Row
import optimizer.util
import pandas
from pandas.testing import assert_frame_equal
import optimizer.algo.hwm
import os
import json
import warnings

class Unittest_Resources_Allocation_1(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
        with open(fpath + '/data_source/bookings_same_attributes.json') as bookings_source:
            self.bookings = json.load(bookings_source)
        with open(fpath + '/data_source/cfg.json') as cfg_source:
            self.cfg = json.load(cfg_source)
        today = '20180402'
        self.days = optimizer.util.get_days_from_bookings(today, self.bookings)
        self.sc = SparkContext.getOrCreate()
        self.hive_context = HiveContext(self.sc)
        self.schema = optimizer.util.get_common_pyspark_schema()

    def compare_two_dfs(self, pandas_df_expected, df_to_test_rows):
        df = self.hive_context.createDataFrame(df_to_test_rows, self.schema)
        df_allocated = optimizer.algo.hwm.hwm_allocation(df, self.bookings, self.days)
        pandas_df_allocated = df_allocated.select("*").toPandas()
        print(pandas_df_expected)
        print(pandas_df_allocated)

        return self.assertTrue(assert_frame_equal(pandas_df_expected, pandas_df_allocated, check_dtype=False) == None)

    def test_hwm_allocation_case1(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3', 'b2'], [], 43562, {'b1': 500, 'b3': 1000, 'b2': 800}]

        rows = [(['20180402', ['b1', 'b3', 'b2'], [], {}, 43562])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case2(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3', 'b2'], [], 43562, {'b1': 250, 'b3': 500, 'b2': 800}]
        pandas_df_expected.loc[1] = ['20180402', ['b1', 'b3'], ['b2'], 43562, {'b1': 250, 'b3': 500}]

        rows = [(['20180402', ['b1', 'b3', 'b2'], [], {}, 43562]), (['20180402', ['b1', 'b3'], ['b2'], {}, 43562])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

if __name__ == '__main__':
    unittest.main()
