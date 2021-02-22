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

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from test_base import TestBase


class TestCleanClickLog(TestBase):

    def test_if_schema_correct(self):
        schema = [('did', 'string'), ('is_click', 'int'), ('action_time', 'string'), ('keyword', 'string'), ('keyword_index', 'int'), 
        ('media', 'string'), ('media_category', 'string'), ('net_type','string'), ('gender', 'int'), ('age', 'int'), ('adv_id', 'string'), 
        ('action_time_seconds', 'int'), ('interval_starting_time', 'int'), ('uckey', 'string'), ('region_id', 'int')]

        self.assertEqual(sorted(self.df_logs.dtypes), sorted(schema))

    def test_if_schema_of_show_click_are_same(self):
        command = "select * from {}".format(self.click_table_name)
        df_click = self.hive_context.sql(command)

        command = "select * from {}".format(self.show_table_name)
        df_show = self.hive_context.sql(command)

        self.assertEqual(sorted(df_click.dtypes), sorted(df_show.dtypes))

    def __if_all_click_items_are_in_show(self, column):
        command = "select * from {}".format(self.click_table_name)
        df = self.hive_context.sql(command)
        click_items = set(df.select(column).distinct().collect())

        command = "select * from {}".format(self.show_table_name)
        df = self.hive_context.sql(command)
        show_items = set(df.select(column).distinct().collect())

        for item in click_items:
            if item not in show_items:
                self.assertTrue(
                    False, '{} {} not in show_log'.format(column, item))

    def test_if_all_click_slot_ids_are_in_show(self):
        self.__if_all_click_items_are_in_show('slot_id')

    def test_interval_starting_time(self):
        pass


if __name__ == "__main__":
    unittest.main()
