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
import sys
import yaml

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum
from pyspark.sql import HiveContext


class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        sc = SparkContext()
        sc.setLogLevel('warn')
        cls.hive_context = HiveContext(sc)

        with open('config.yml', 'r') as ymlfile:
            cfg = yaml.load(ymlfile)

        cfg_clean = cfg['pipeline']['main_clean']
        cls.persona_table_name = cfg_clean['data_output']['persona_output_table_name']
        cls.show_table_name = cfg_clean['data_output']['showlog_output_table_name']
        cls.click_table_name = cfg_clean['data_output']['clicklog_output_table_name']

        cfg_main_trainready = cfg['pipeline']['main_trainready']
        trainready_table_name = cfg_main_trainready['trainready_output_table_name']

        cfg_main_logs = cfg['pipeline']['main_logs']
        logs_table_name = cfg_main_logs['logs_output_table_name']

        command = "select * from {}".format(logs_table_name)
        cls.df_logs = cls.hive_context.sql(command)

        command = "select * from {}".format(trainready_table_name)
        cls.df_tr = cls.hive_context.sql(command)

        cls.conditions = {
            'new_slot_id_list': [
                '06',
                '11',
                '05',
                '04',
                '03',
                '02',
                '01',
                'l03493p0r3',
                'x0ej5xhk60kjwq',
                'g7m2zuits8',
                'w3wx3nv9ow5i97',
                'a1nvkhk62q',
                'g9iv6p4sjy',
                'c4n08ku47t',
                'b6le0s4qo8',
                'd9jucwkpr3',
                'p7gsrebd4m',
                'a8syykhszz',
                'l2d4ec6csv',
                'j1430itab9wj3b',
                's4z85pd1h8',
                'z041bf6g4s',
                '71bcd2720e5011e79bc8fa163e05184e',
                'a47eavw7ex',
                '68bcd2720e5011e79bc8fa163e05184e',
                '66bcd2720e5011e79bc8fa163e05184e',
                '72bcd2720e5011e79bc8fa163e05184e',
                'f1iprgyl13',
                'q4jtehrqn2',
                'm1040xexan',
                'd971z9825e',
                'a290af82884e11e5bdec00163e291137',
                'w9fmyd5r0i',
                'x2fpfbm8rt',
                'e351de37263311e6af7500163e291137',
                'k4werqx13k',
                '5cd1c663263511e6af7500163e291137',
                '17dd6d8098bf11e5bdec00163e291137',
                'd4d7362e879511e5bdec00163e291137',
                '15e9ddce941b11e5bdec00163e291137'
            ],
            'new_slot_id_app_name_list': [
                'Huawei Magazine',
                'Huawei Magazine',
                'Huawei Magazine',
                'Huawei Magazine',
                'Huawei Magazine',
                'Huawei Magazine',
                'Huawei Magazine',
                'Huawei Brower',
                'Huawei Video',
                'Huawei Video',
                'Huawei Video',
                'Huawei Music',
                'Huawei Music',
                'Huawei Music',
                'Huawei Music',
                'Huawei Reading',
                'Huawei Reading',
                'Huawei Reading',
                'Huawei Reading',
                'Video 1.0',
                'Video 2.0',
                'Tencent Video',
                'AI assistant',
                'AI assistant',
                'AI assistant',
                'AI assistant',
                'Huawei Video',
                'Huawei Video',
                'Huawei Video',
                'Video 1.0',
                'Themes',
                'Huawei Music',
                'Huawei Reading',
                'Huawei Reading',
                'Huawei Reading',
                'Huawei Reading',
                'Honor Reading',
                'Video 1.0',
                'Video 2.0',
                'HiSkytone'
            ],
            'starting_date': '2019-12-19',
            'ending_date': '2020-04-15'
        }

    def compare_dfs(self, df1, df2):
        col1 = sorted(df1.collect())
        col2 = sorted(df2.collect())
        return col1 == col2
