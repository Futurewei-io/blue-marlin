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

# Testing hwm_allocation() with bookings in natural order.

import unittest
from imscommon.es.ims_esclient import ESClient
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
import optimizer.util
import pandas
from pandas.testing import assert_frame_equal
import optimizer.algo.hwm
import os
import json
import warnings

class Unittest_HWM_Allocations_2(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
        with open(fpath + '/data_source/bookings_fully_overlapped.json') as bookings_source:
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
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3', 'b2'], [], 733, {'b1': 500, 'b3': 233}]
        
        df_to_test_rows = [(['20180402', ['b1', 'b3', 'b2'], [], {}, 733])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case2(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1'], ['b2', 'b3'], 6047, {'b1': 500}]

        df_to_test_rows = [(['20180402', ['b1'], ['b2', 'b3'], {}, 6047])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case3(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b2'], ['b1', 'b3'], 1410, {'b2': 800}]

        df_to_test_rows = [(['20180402', ['b2'], ['b1', 'b3'], {}, 1410])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case4(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b3'], ['b1', 'b2'], 12241, {'b3': 1000}]

        df_to_test_rows = [(['20180402', ['b3'], ['b1', 'b2'], {}, 12241])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case5(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b2'], ['b3'], 3575, {'b1': 500, 'b2': 800}]

        df_to_test_rows = [(['20180402', ['b1', 'b2'], ['b3'], {}, 3575])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case6(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b2', 'b3'], ['b1'], 1002, {'b3': 1000, 'b2': 2}]

        df_to_test_rows = [(['20180402', ['b2', 'b3'], ['b1'], {}, 1002])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case7(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3'], ['b2'], 11181, {'b1': 500, 'b3': 1000}]

        df_to_test_rows = [(['20180402', ['b1', 'b3'], ['b2'], {}, 11181])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case8(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3', 'b2'], [], 733, {'b2': 640, 'b3': 62, 'b1': 31}]
        pandas_df_expected.loc[1] = ['20180402', ['b1', 'b3'], ['b2'], 11181, {'b3': 938, 'b1': 469}]

        df_to_test_rows = [(['20180402', ['b1', 'b3', 'b2'], [], {}, 733]), (['20180402', ['b1', 'b3'], ['b2'], {}, 11181])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case9(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3'], ['b2'], 3575, {'b1': 500, 'b3': 754}]
        pandas_df_expected.loc[1] = ['20180402', ['b2', 'b3'], ['b1'], 1002, {'b3': 246, 'b2': 756}]

        df_to_test_rows = [(['20180402', ['b1', 'b3'], ['b2'], {}, 3575]), (['20180402', ['b2', 'b3'], ['b1'], {}, 1002])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case10(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b1'], ['b2', 'b3'], 6047, {'b1': 500}]
        pandas_df_expected.loc[1] = ['20180402', ['b2'], ['b1', 'b3'], 1410, {'b2': 800}]

        df_to_test_rows = [(['20180402', ['b1'], ['b2', 'b3'], {}, 6047]), (['20180402', ['b2'], ['b1', 'b3'], {}, 1410])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

if __name__ == '__main__':
    unittest.main()
