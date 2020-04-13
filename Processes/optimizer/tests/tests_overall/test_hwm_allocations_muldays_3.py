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

# the following testcases test in different days.
# Baohua Cao
# Testing hwm_allocation() with sorted bookings.

import unittest
from imscommon.es.ims_esclient import ESClient
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType, Row
import optimizer.util
import pandas
from pandas.testing import assert_frame_equal
import optimizer.algo.hwm
import warnings

class Unittest_HWM_Allocation_MulDays3(unittest.TestCase):
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
        # Print out the name of the current testing function to track logs
        # Testcase type: 1 booking bucket with 1 booking_id in ands
        # testcase 1: booking bucket ['20180402', ['b80'], ['b60'], {}, 3239]
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b80'], ['b60'], 3239, {'b80': 18}]

        df_to_test_rows =  = [(['20180402', ['b80'], ['b60'], {}, 3239])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case2(self):
        # bk_id: b11, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 11
        # bk_id: b12, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 12
        # Testcase type: 1 booking bucket with 2 booking_id in ands
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b120', 'b110'], [], 8900, {'b120': 2, 'b110': 2}]

        df_to_test_rows =  = [(['20180402', ['b120', 'b110'], [], {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case3(self):
        # Testcase type: 1 booking bucket with 3 booking_id in ands
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b60', 'b70', 'b100'], [], 8900, {'b60': 13, 'b70': 15, 'b100': 20}]

        df_to_test_rows =  = [(['20180402', ['b60', 'b70', 'b100'], [], {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case4(self):
        # Testcase type: 2 booking buckets with 4 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b80'], ['b60', 'b70', 'b90'], 3239, {'b80': 18}]
        pandas_df_expected.loc[1] = ['20180402', ['b60', 'b70'], ['b80', 'b90'], 8900, {'b60': 13, 'b70': 15}]

        df_to_test_rows =  = [(['20180402', ['b80'], ['b60', 'b70', 'b90'], {}, 3239]), (['20180402', ['b60', 'b70'], ['b80', 'b90'], {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case5(self):
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b60', 'b70', 'b100', 'b110', 'b120'], ['b80', 'b90'], 
        8900, {'b60': 13, 'b70': 15, 'b100': 20, 'b110': 2, 'b120': 2}]

        df_to_test_rows =  = [(['20180402', ['b60', 'b70', 'b100', 'b110', 'b120'], ['b80', 'b90'],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case6(self):
        # bk_id: b13, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 130000
        # bk_id: b12, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 12
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b130', 'b120'], [], 
        8900, {'b130': 8900}]

        df_to_test_rows =  = [(['20180402', ['b130', 'b120'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)
 
    def test_hwm_allocation_case7(self):
        # bk_id: b15, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 8900
        # bk_id: b14, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 8900
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b150', 'b140'], [], 8900, {'b150': 1780, 'b140': 1780}]

        df_to_test_rows =  = [(['20180402', ['b150', 'b140'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case8(self):
        # bk_id: b17, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4450
        # bk_id: b16, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4450
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b170', 'b160'], [], 8900, {'b170': 890, 'b160': 890}]

        df_to_test_rows =  = [(['20180402', ['b170', 'b160'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case9(self):
        # bk_id: b18, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4451
        # bk_id: b17, days: ['20180401', '20180402', '20180403', '20180404', '20180405'], a: ['4'], g: ['g_f'], si: ['2'], amount: 4450
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b180', 'b170'], [], 8900, {'b180': 890, 'b170': 890}]

        df_to_test_rows =  = [(['20180402', ['b180', 'b170'], [],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

    def test_hwm_allocation_case10(self):
        # Testcase type: 3 booking buckets with 5 bookings included.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'amount', 'allocated'])
        pandas_df_expected.loc[0] = ['20180402', ['b60', 'b70', 'b100', 'b120', 'b160', 'b170', 'b180'], ['b80', 'b90'], 
        8900, {'b120': 2, 'b100': 20, 'b160': 890, 'b170': 890, 'b180': 890, 'b70': 15, 'b60': 13}] # b6, b7, b10, b12, b16, b17, b18 have the same attributes.

        df_to_test_rows =  = [(['20180402', ['b60', 'b70', 'b100', 'b120', 'b160', 'b170', 'b180'], ['b80', 'b90'],  {}, 8900])]
        return self.compare_two_dfs(pandas_df_expected, df_to_test_rows)

if __name__ == '__main__':
    unittest.main()
