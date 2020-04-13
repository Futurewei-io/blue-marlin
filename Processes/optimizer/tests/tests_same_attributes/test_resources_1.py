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

# Testing resources generation based on b1, b2, b3 with idnex of bookings_03242020.
# Baohua Cao

import unittest
from imscommon.es.ims_esclient import ESClient
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType, Row
import optimizer.util
import pandas
from pandas.testing import assert_frame_equal
import optimizer.main
import optimizer.dao.query_builder
import json
import os
import warnings

class Testing_Resources_Generation_1(unittest.TestCase):
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
        self.schema = optimizer.util.get_common_pyspark_self.schema()
        self.bookings_map = optimizer.util.get_bookings_map(self.bookings)

    def test_resources_count(self):
        df = self.hive_context.createDataFrame(self.sc.emptyRDD(), self.schema)
        df = optimizer.main.generate_resources(self.cfg, df, self.bookings_map, self.days, self.bookings, self.hive_context)
        self.assertTrue(df.count() == 1)

    def test_resources_df(self):
        df = self.hive_context.createDataFrame(self.sc.emptyRDD(), self.schema)
        df = optimizer.main.generate_resources(self.cfg, df, self.bookings_map, self.days, self.bookings, self.hive_context)

        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3', 'b2'], [], {}, 43562]
        pandas_df_generated = df.select("*").toPandas()
        print(pandas_df_expected)
        print(pandas_df_generated)

        self.assertTrue(assert_frame_equal(pandas_df_expected, pandas_df_generated, check_dtype=False) == None)

    def test_resources_split_1(self):
        #testing b1 and b3 generated a resource of b1 && b3.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3'], [], {}, 43562]
        
        new_bk_id = 'b3'
        rows = [('20180402', ['b1'], [], {}, 0)]
        booking_spark_df = self.hive_context.createDataFrame(rows, self.schema)
        spark_df_splitted_rdd = booking_spark_df.rdd.flatMap(optimizer.main.bb_split_method(self.cfg, self.bookings_map, new_bk_id))
        spark_df_splitted = self.hive_context.createDataFrame(spark_df_splitted_rdd, self.schema)
        pandas_df_splitted = spark_df_splitted.select("*").toPandas()
        print(pandas_df_expected)
        print(pandas_df_splitted)

        self.assertTrue(assert_frame_equal(pandas_df_expected, pandas_df_splitted, check_dtype=False) == None)

    def test_resources_split_2(self):
        #testing b1, b3 and b2 generated a resource of b1 && b3 && b2.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180402', ['b1', 'b3', 'b2'], [], {}, 43562]
        
        new_bk_id = 'b2'
        rows = [('20180402', ['b1', 'b3'], [], {}, 43562)]
        booking_spark_df = self.hive_context.createDataFrame(rows, self.schema)
        spark_df_splitted_rdd = booking_spark_df.rdd.flatMap(optimizer.main.bb_split_method(self.cfg, self.bookings_map, new_bk_id))
        spark_df_splitted = self.hive_context.createDataFrame(spark_df_splitted_rdd, self.schema)
        pandas_df_splitted = spark_df_splitted.select("*").toPandas()
        print(pandas_df_expected)
        print(pandas_df_splitted)

        self.assertTrue(assert_frame_equal(pandas_df_expected, pandas_df_splitted, check_dtype=False) == None)


if __name__ == '__main__':
    unittest.main()
