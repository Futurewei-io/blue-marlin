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

# Testing resources generation based on b1, b2, b3 with idnex of bookings_04082020.
# Baohua Cao

import unittest
from imscommon.es.ims_esclient import ESClient
from pyspark.sql import HiveContext
from pyspark import SparkContext
import optimizer.util
import pandas
from pandas.testing import assert_frame_equal
import optimizer.main
import json
import os
import warnings

class Testing_Resources_Generation_2(unittest.TestCase):
    def setUp(self):
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
        self.bookings_map = optimizer.util.get_bookings_map(self.bookings)

    def test_resources_count(self):
        df = self.hive_context.createDataFrame(self.sc.emptyRDD(), self.schema)
        df = optimizer.main.generate_resources(self.cfg, df, self.bookings_map, self.days, self.bookings, self.hive_context)
        self.assertTrue(df.count() == 7)

    def test_resource_1(self):
        df = self.hive_context.createDataFrame(self.sc.emptyRDD(), self.schema)
        df = optimizer.main.generate_resources(self.cfg, df, self.bookings_map, self.days, self.bookings, self.hive_context)

        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3', 'b2'], [], {}, 733]
        pandas_df_expected.loc[1] = ['20180402', ['b1', 'b3'], ['b2'], {}, 11181]
        pandas_df_expected.loc[2] = ['20180402', ['b1', 'b2'], ['b3'], {}, 3575]
        pandas_df_expected.loc[3] = ['20180402', ['b1'], ['b3', 'b2'], {}, 6047]
        pandas_df_expected.loc[4] = ['20180402', ['b3', 'b2'], ['b1'], {}, 1002]
        pandas_df_expected.loc[5] = ['20180402', ['b3'], ['b1', 'b2'], {}, 12241]
        pandas_df_expected.loc[6] = ['20180402', ['b2'], ['b1', 'b3'], {}, 1410]
        pandas_df_generated = df.select("*").toPandas()

        self.assertTrue(assert_frame_equal(pandas_df_expected, pandas_df_generated, check_dtype=False) == None)

if __name__ == '__main__':
    unittest.main()
