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

import unittest
from din_model.pipeline import main_clean, util
from test_base import TestBase


class TestMainClean(TestBase):

    def test_clean_persona_1(self):
        """
        tests policy one which is to remove DIDs that are associated to different pairs of (gender,age)
        """
        df_raw_persona = self.hive_context.createDataFrame(
            [
                ('1000', '0', '1'),
                ('1001', '1', '2'),
                ('1002', '0', '3'),
                ('1000', '0', '4')
            ],
            ['did', 'gender', 'age']
        )
        df_expected = self.hive_context.createDataFrame(
            [
                ('1001', 1, 2),
                ('1002', 0, 3)
            ],
            ['did', 'gender', 'age']
        )
        columns = ['did', 'gender', 'age']
        df = main_clean.clean_persona(df_raw_persona)
        self.assertTrue(self.compare_dfs(df.select(columns), df_expected.select(columns)))

    def test_clean_batched_log(self):
        """
        Refer to clean-batched-log method docstring
        """

        df_showlog = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:04:06.213'),
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613'),
                ('1003', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613')
            ],
            ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'price_model', 'action_time']
        )

        # This is cleaned persona
        df_persona = self.hive_context.createDataFrame(
            [
                ('1001', 1, 2),
                ('1002', 0, 3),
                ('1004', 1, 3)
            ],
            ['did', 'gender', 'age']
        )

        conditions = self.conditions

        df_keywords = self.hive_context.createDataFrame(
            [
                ('C10499309', 'video'),
                ('C10295678', 'info')
            ],
            ['spread_app_id', 'keyword']
        )

        df_expected = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113', 1, 2, 'video'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413', 0, 3, 'info'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:04:06.213', 1, 2, 'info'),
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613', 1, 2, 'video')
            ],
            ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'price_model', 'action_time', 'gender', 'age', 'keyword']
        )

        columns = df_expected.columns
        df = main_clean.clean_batched_log(df=df_showlog, df_persona=df_persona, conditions=conditions, df_keywords=df_keywords)

        self.assertTrue(self.compare_dfs(df.select(columns), df_expected.select(columns)))

    def test_clean_logs_1(self):
        """
        Refer to clean-log method docstring

        1. check if the tables are exist and drop them
        2. take the table and two dataframe (keyword and persona)
        3. clean up based on the dids and add persona's feature to the new dataframe
        4. check if the expected data frame is the same as the dataframe which created by clean_logs or not

        """
        self.hive_context.sql("DROP TABLE if exists show_test_0000098989")
        self.hive_context.sql("DROP TABLE if exists show_output_test_0000098989")
        self.hive_context.sql("DROP TABLE if exists click_test_0000098989")
        self.hive_context.sql("DROP TABLE if exists click_output_test_0000098989")

        cfg_test = {
            'log': {'level': 'WARN'},
            'pipeline': {
                'main_clean': {'conditions': {
                    'new_slot_id_list': ['a47eavw7ex'],
                    'new_slot_id_app_name_list': ['Huawei Reading',
                                                  'Honor Reading',
                                                  'Video 1.0',
                                                  'Video 2.0',
                                                  'HiSkytone'],
                    'starting_date': '2019-12-19',
                    'ending_date': '2020-04-15'},
                    'data_input': {'load_logs_in_minutes': 144000}

                }}}

        df_showlog_test = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213'),
                ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613'),
                ('1003', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613')
            ],
            ['did', 'adv_id', 'adv_type', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'adv_bill_mode_cd',
             'show_time']
        )
        util.write_to_table(df_showlog_test, 'show_test_0000098989', mode='overwrite')

        df_clicklog_test = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:03:46.113'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:03:46.413'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-03 08:04:06.213'),
                ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:05:00.613'),
                ('1003', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:05:00.613')
            ],
            ['did', 'adv_id', 'adv_type', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'adv_bill_mode_cd',
             'click_time']
        )
        util.write_to_table(df_clicklog_test, 'click_test_0000098989', mode='overwrite')

        df_persona = self.hive_context.createDataFrame([
            ('1001', 1, 2),
            ('1002', 0, 3),
            ('1004', 1, 3)
        ],
            ['did', 'gender', 'age']
        )
        df_keywords = self.hive_context.createDataFrame(
            [
                ('C10499309', 'video'),
                ('C10295678', 'info')
            ],
            ['spread_app_id', 'keyword']
        )

        df_expected = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113', 1, 2, 'video'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413', 0, 3, 'info'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213', 1, 2, 'info'),
            ],
            ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'price_model', 'action_time', 'gender', 'age', 'keyword']
        )

        columns = ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'price_model', 'action_time', 'gender', 'age', 'keyword']

        #showlog_table, showlog_output_table, clicklog_table, clicklog_output_table
        table_name = ("show_test_0000098989", "show_output_test_0000098989", "click_test_0000098989", "click_output_test_0000098989")
        main_clean.clean_logs(cfg=cfg_test, df_persona=df_persona, df_keywords=df_keywords, log_table_names=table_name)
        command = "select * from show_output_test_0000098989"
        df = self.hive_context.sql(command)

        self.assertTrue(self.compare_dfs(df.select(columns), df_expected.select(columns)))

    def test_clean_logs_2(self):
        """
        Refer to clean-log method docstring

        *** the same as test_clean_log_2 ***
        + checks if it works well when the end date is in the middle of the table's time frame or not
        """
        self.hive_context.sql("DROP TABLE if exists show_test_00000989899")
        self.hive_context.sql("DROP TABLE if exists show_output_test_00000989899")
        self.hive_context.sql("DROP TABLE if exists click_test_00000989899")
        self.hive_context.sql("DROP TABLE if exists click_output_test_00000989899")

        cfg_test = {
            'log': {'level': 'WARN'},
            'pipeline': {
                'main_clean': {'conditions': {
                    'new_slot_id_list': ['a47eavw7ex'],
                    'new_slot_id_app_name_list': ['Huawei Reading',
                                                  'Honor Reading',
                                                  'Video 1.0',
                                                  'Video 2.0',
                                                  'HiSkytone'],
                    'starting_date': '2019-12-19',
                    'ending_date': '2020-04-05'},
                    'data_input': {'load_logs_in_minutes': 144000}

                }}}

        df_showlog_test = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213'),
                ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613'),
                ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-06 08:05:00.613'),
                ('1003', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613')
            ],
            ['did', 'adv_id', 'adv_type', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'adv_bill_mode_cd',
             'show_time']
        )
        util.write_to_table(df_showlog_test, 'show_test_00000989899', mode='overwrite')

        df_clicklog_test = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:03:46.113'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:03:46.413'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-03 08:04:06.213'),
                ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:05:00.613'),
                ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-06 08:05:00.613'),
                ('1003', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC',
                 '2020-04-02 08:05:00.613')
            ],
            ['did', 'adv_id', 'adv_type', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'adv_bill_mode_cd',
             'click_time']
        )
        util.write_to_table(df_clicklog_test, 'click_test_00000989899', mode='overwrite')

        df_persona = self.hive_context.createDataFrame([
            ('1001', 1, 2),
            ('1002', 0, 3),
            ('1004', 1, 3)
        ],
            ['did', 'gender', 'age']
        )
        df_keywords = self.hive_context.createDataFrame(
            [
                ('C10499309', 'video'),
                ('C10295678', 'info')
            ],
            ['spread_app_id', 'keyword']
        )

        df_expected = self.hive_context.createDataFrame(
            [
                ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113', 1, 2, 'video'),
                ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413', 0, 3, 'info'),
                ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213', 1, 2, 'info'),
            ],
            ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'price_model', 'action_time', 'gender', 'age', 'keyword']
        )

        columns = ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id', 'device_name', 'net_type', 'price_model', 'action_time', 'gender', 'age', 'keyword']

        #showlog_table, showlog_output_table, clicklog_table, clicklog_output_table
        table_name = ("show_test_00000989899", "show_output_test_00000989899", "click_test_00000989899", "click_output_test_00000989899")
        main_clean.clean_logs(cfg=cfg_test, df_persona=df_persona, df_keywords=df_keywords, log_table_names=table_name)
        command = "select * from show_output_test_00000989899"
        df = self.hive_context.sql(command)

        self.assertTrue(self.compare_dfs(df.select(columns), df_expected.select(columns)))


if __name__ == "__main__":
    unittest.main()
