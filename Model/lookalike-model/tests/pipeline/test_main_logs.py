# Copyright 2021, Futurewei Technologies
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
import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext
from lookalike_model.pipeline import main_logs, util
from data_generator import *

class TestMainLogs(unittest.TestCase):

    def setUp (self):
        # Set the log level.
        sc = SparkContext.getOrCreate()
        sc.setLogLevel('ERROR')

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.hive_context = HiveContext(sc)

    def test_join_logs (self):
        print('*** Running test_join_logs test ***')

        # Load the configuration data.
        with open('pipeline/config_logs.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        showlog_table = cfg['pipeline']['main_clean']['showlog_output_table']
        clicklog_table = cfg['pipeline']['main_clean']['clicklog_output_table']
        log_output_table = cfg['pipeline']['main_logs']['logs_output_table_name']
        log_table_names = (showlog_table, clicklog_table, log_output_table)

        did_bucket_num = cfg['pipeline']['main_clean']['did_bucket_num']
        interval_time_in_seconds = cfg['pipeline']['main_logs']['interval_time_in_seconds']

        # Create the input data tables.
        create_log_table(self.spark, clicklog_table)
        create_log_table(self.spark, showlog_table)

        # Clear the output from any previous runs.
        util.drop_table(self.hive_context, log_output_table)

        # Run the method to be tested.
        main_logs.join_logs(self.hive_context, util.load_batch_config(cfg), interval_time_in_seconds, log_table_names, did_bucket_num)

        # Validate the output.
        df = util.load_df(self.hive_context, log_output_table)
        self.validate_unified_logs(df, create_cleaned_log(self.spark))

    def test_run (self):
        print('*** Running test_run test ***')

        # Load the configuration data.
        with open('pipeline/config_logs.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        # Create the input data tables.
        showlog_table = cfg['pipeline']['main_clean']['showlog_output_table']
        clicklog_table = cfg['pipeline']['main_clean']['clicklog_output_table']
        create_log_table(self.spark, clicklog_table)
        create_log_table(self.spark, showlog_table)

        # Clear the output from any previous runs.
        log_output_table = cfg['pipeline']['main_logs']['logs_output_table_name']
        util.drop_table(self.hive_context, log_output_table)

        # Run the method to be tested.
        main_logs.run(self.hive_context, cfg)

        # Validate the output.
        df = util.load_df(self.hive_context, log_output_table)
        print_df_generator_code(df.sort('did', 'is_click'))
        self.validate_unified_logs(df, create_cleaned_log(self.spark))


    def validate_unified_logs (self, df, df_log):
        # Verify the column names.
        columns = ['is_click', 'did', 'adv_id', 'media', 
            'net_type', 'action_time', 'gender', 'age', 
            'keyword', 'keyword_index', 'day', 'did_bucket']
        for name in columns:
            self.assertTrue(name in df.columns)

        self.assertEqual(df.count(), 2*df_log.count())

        df = df.sort('did', 'is_click')
        rows = df.collect()
        for i in range(0, len(rows), 2):
            for column in columns:
                if column == 'is_click':
                    self.assertNotEqual(rows[i][column], rows[i+1][column])
                else:
                    self.assertEqual(rows[i][column], rows[i+1][column])



# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()
