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
import json
import os
import warnings

class Unittest_HWM_Allocation_NaturalOrder2(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
        with open(fpath + '/data_source/bookings_overall.json') as bookings_source:
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
        # Testcase type: 1 booking bucket with 1 booking_id in ands
        # testcase 1: booking bucket ['20180405', ['b8'], ['b6'], {}, 3239]
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b8'], ['b6'], 3239, {'b8': 88}]

        df_to_test_rows = [(['20180405', ['b8'], ['b6'], {}, 3239])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case2(self):
        # bk_id: b13, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 130000
        # bk_id: b12, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 12
        # Testcase type: 1 booking bucket with 2 booking_id in ands
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b11', 'b12'], [], 8900, {'b11': 11, 'b12': 12}]

        df_to_test_rows = [(['20180405', ['b11', 'b12'], [], {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case3(self):
        # Testcase type: 1 booking bucket with 3 booking_id in ands
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b6', 'b7', 'b10'], [], 8900, {'b6': 66, 'b7': 77, 'b10': 100}]

        df_to_test_rows = [(['20180405', ['b6', 'b7', 'b10'], [], {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case4(self):
        # Testcase type: 2 booking buckets with 4 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b8'], ['b6', 'b7', 'b9'], 3239, {'b8': 88}]
        pandas_df_expected.loc[1] = ['20180405', ['b6', 'b7'], ['b8', 'b9'], 8900, {'b6': 66, 'b7': 77}]

        df_to_test_rows = [(['20180405', ['b8'], ['b6', 'b7', 'b9'], {}, 3239]), (['20180405', ['b6', 'b7'], ['b8', 'b9'], {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case5(self):
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b6', 'b7', 'b10', 'b11', 'b12'], ['b8', 'b9'], 
        8900, {'b11': 11, 'b6': 66, 'b7': 77, 'b10': 100, 'b12': 12}]

        df_to_test_rows = [(['20180405', ['b6', 'b7', 'b10', 'b11', 'b12'], ['b8', 'b9'],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case6(self):
        # bk_id: b13, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 130000
        # bk_id: b12, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 12
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b13', 'b12'], [], 
        8900, {'b13': 8900}]

        df_to_test_rows = [(['20180405', ['b13', 'b12'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)
 
    def test_hwm_allocation_case7(self):
        # bk_id: b15, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 8900
        # bk_id: b14, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 8900
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b15', 'b14'], [], 8900, {'b15': 8900}]

        df_to_test_rows = [(['20180405', ['b15', 'b14'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case8(self):
        # bk_id: b17, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4450
        # bk_id: b16, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4450
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b16', 'b17'], [], 8900, {'b16': 4450, 'b17': 4450}]

        df_to_test_rows = [(['20180405', ['b16', 'b17'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case9(self):
        # bk_id: b18, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4451
        # bk_id: b17, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4450
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b17', 'b18'], [], 8900, {'b17': 4450, 'b18': 4450}]

        df_to_test_rows = [(['20180405', ['b17', 'b18'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case10(self):
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180405', ['b16', 'b17', 'b6', 'b7', 'b10', 'b12', 'b18'], ['b8', 'b9'], 
        8900, {'b16': 4450, 'b17': 4450}] # b6, b7, b10, b12, b16, b17, b18 have the same attributes.

        df_to_test_rows = [(['20180405', ['b16', 'b17', 'b6', 'b7', 'b10', 'b12', 'b18'], ['b8', 'b9'],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

if __name__ == '__main__':
    unittest.main()
