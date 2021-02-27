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

# Testing Optimizer's split booking buckets.

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

class Testing_Resources_Split_1(unittest.TestCase):
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
        self.bookings_map = optimizer.util.get_bookings_map(self.bookings)

    def compare_splitted_dfs(self, pandas_df_expected, rows, new_bk_id):
        booking_spark_df = self.hive_context.createDataFrame(rows, self.schema)
        spark_df_splitted_rdd = booking_spark_df.rdd.flatMap(optimizer.main.bb_split_method(self.cfg, self.bookings_map, new_bk_id))
        spark_df_splitted = self.hive_context.createDataFrame(spark_df_splitted_rdd, self.schema)
        pandas_df_splitted = spark_df_splitted.select("*").toPandas()
        print(pandas_df_expected)
        print(pandas_df_splitted)

        self.assertTrue(assert_frame_equal(pandas_df_expected, pandas_df_splitted, check_dtype=False) == None)

    # es: elasticsearch
    def test_es_bookings_search(self):
        self.assertTrue(len(self.bookings) >= 3)

    def test_es_predictions_search(self):
        es_client_predictions = ESClient(self.cfg['es_host'], self.cfg['es_port'],
                                         self.cfg['es_predictions_index'], self.cfg['es_predictions_type'])
        predictions = es_client_predictions.search({"size": 100})
        
        self.assertTrue(len(predictions) > 0)
        self.assertTrue(len(predictions) >= 40)
    
    def test_get_tbr_ratio(self):
        es_client_tbr = ESClient(self.cfg['es_host'], self.cfg['es_port'],
                                 self.cfg['es_tbr_index'], self.cfg['es_tbr_type'])
        ands = ['b6','b7']
        get_tbr_ratio = optimizer.dao.query_builder.get_tbr_ratio(
            ands, self.bookings_map, es_client_tbr)
        
        print('get_tbr_ratio='+str(get_tbr_ratio))
        self.assertTrue(get_tbr_ratio == 1.0)

    def test_bb_split_method_case1(self):
        # Testcase type: 1 booking is splitted by another different booking
        # testcase 1: booking b8 is splitted with a new booking b6.
        # bk_id: b8, days: ['20180405'], a: ['1'], g: ['g_X'], si: ['1']
        # bk_id: b6, days: ['20180405'], a: ['4'], g: ['g_f'], si: ['2']
        # original dataframe: ['20180405', ['b8'], [], {}, 0]
        # splitted to : d1: ands=['b8', 'b6'], minus=[]  &&  d2: ands=['b8'], minus=['b6']
        # in this testecase by hand, d2 has the get_bb_count() value 3239, so d2 is valid, but d1 is invalid.
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180405', ['b8'], ['b6'], {}, 3239]

        day, booking_id, new_bk_id = '20180405', 'b8', 'b6'

        rows = [(day, [booking_id], [], {}, 0)]
        return self.compare_splitted_dfs(pandas_df_expected, rows, new_bk_id)

    def test_bb_split_method_case2(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180405', ['b6', 'b7'], [], {}, 8900]

        day, booking_id, new_bk_id = '20180405', 'b6', 'b7'
        rows = [(day, [booking_id], [], {}, 0)]
        return self.compare_splitted_dfs(pandas_df_expected, rows, new_bk_id)

    def test_bb_split_method_case3(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180405', ['b8'], ['b6', 'b7'], {}, 3239]
        pandas_df_expected.loc[1] = ['20180405', ['b6', 'b7'], ['b8'], {}, 8900]

        new_bk_id = 'b7'
        rows = [('20180405', ['b8'], ['b6'], {}, 3239), ('20180405', ['b6'], ['b8'], {}, 0)]
        return self.compare_splitted_dfs(pandas_df_expected, rows, new_bk_id)
    
    def test_bb_split_method_case4(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180405', ['b6', 'b7', 'b10'], [], {}, 8900]

        new_bk_id = 'b10'
        rows = [('20180405', ['b6', 'b7'], [], {}, 8900)]
        return self.compare_splitted_dfs(pandas_df_expected, rows, new_bk_id)

    def test_bb_split_method_case5(self):
        pandas_df_expected = pandas.DataFrame(columns=['day', 'ands', 'minus', 'allocated', 'amount'])
        pandas_df_expected.loc[0] = ['20180405', ['b8'], ['b6', 'b7', 'b9'], {}, 3239]
        pandas_df_expected.loc[1] = ['20180405', ['b6', 'b7'], ['b8', 'b9'], {}, 8900]

        new_bk_id = 'b9'
        rows = [('20180405', ['b8'], ['b6', 'b7'], {}, 3239), ('20180405', ['b6', 'b7'], ['b8'], {}, 8900)]
        return self.compare_splitted_dfs(pandas_df_expected, rows, new_bk_id)

if __name__ == '__main__':
    unittest.main()
