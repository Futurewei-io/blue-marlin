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
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from lookalike_model.pipeline import main_clean, util
from data_generator import *

class TestMainClean(unittest.TestCase):

    def setUp (self):
        # Set the log level.
        sc = SparkContext.getOrCreate()
        sc.setLogLevel('ERROR')

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.hive_context = HiveContext(sc)


    # Testing the method that tests user uniqueness and removes users with conflicting age/gender.
    def test_clean_persona (self):
        print('*** Running test_clean_persona ***')

        # Get the persona data to be cleaned.
        df = create_raw_persona(self.spark)

        # Run the method to be tested.
        bucket_num = 4
        df = main_clean.clean_persona(df, bucket_num)
        print(df.show(100, False))

        # Validate the output.
        self.validate_clean_persona(df, bucket_num)

    # Tests the method that adds the 'day' column with just the date from the action_time column.
    def test_add_day (self):
        print('*** Running test_add_day ***')

        # Get the log data frame.
        df = create_raw_log(self.spark)

        # Run the method to be tested.
        df = main_clean.add_day(df)
        print(df.show(100, False))

        # Validate the output.
        self.validate_add_day(df)

    # Testing the addition of the 'did_bucket' column with a hash mod bucket_num value.
    def test_add_did_bucket (self):
        print('*** Running test_add_did_bucket ***')

        # Get the data 
        df = create_raw_persona(self.spark)

        # Run the method to be tested.
        bucket_num = 4
        df = main_clean.add_did_bucket(df, bucket_num)
        print(df.show(100, False))

        # Validate the output.
        self.validate_add_did_bucket(df, bucket_num)

    # Testing the method that joins the log rows with the user persona, keyword, and media category.
    def test_clean_batched_log (self):
        print('*** Running test_clean_batched_log ***')

        # Get the data inputs for the test.
        df_log = create_raw_log(self.spark)
        df_persona = create_cleaned_persona(self.spark)
        df_keywords = create_keywords(self.spark)

        conditions = {
            'new_slot_id_list': [
                'abcdef0', 'abcdef1', 'abcdef2', 'abcdef3', 'abcdef4', 
                'abcdef5', 'abcdef6', 'abcdef7', 'abcdef8', 'abcdef9'
            ],
            'new_slot_id_app_name_list': [
                'Huawei Magazine', 'Huawei Browser', 'Huawei Video', 'Huawei Music', 'Huawei Reading', 
                'Huawei Magazine', 'Huawei Browser', 'Huawei Video', 'Huawei Music', 'Huawei Reading'
            ]
        }

        # Run the method to be tested.
        bucket_num = 4
        df = main_clean.clean_batched_log(df_log, df_persona, conditions, df_keywords, bucket_num)
        print(df.sort('did').show(100, False))

        # Validate the output.
        self.validate_cleaned_log(df, conditions, df_persona, df_keywords, df_log, bucket_num)

    # Testing data look up and cleaning process for clicklog and showlog data.
    def test_clean_logs (self):
        print('*** Running test_clean_logs ***')
        with open('pipeline/config_clean.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        showlog_table = cfg['showlog_table_name']
        showlog_output_table = cfg['pipeline']['main_clean']['showlog_output_table']
        clicklog_table = cfg['clicklog_table_name']
        clicklog_output_table = cfg['pipeline']['main_clean']['clicklog_output_table']
        log_table_names = (showlog_table, showlog_output_table, clicklog_table, clicklog_output_table)
        print(showlog_table)
        print(clicklog_table)
        print(showlog_output_table)
        print(clicklog_output_table)

        # Create the persona and keyword dataframes.
        df_persona = create_cleaned_persona(self.spark)
        df_keywords = create_keywords(self.spark)

        # Create the clicklog and showlog tables.
        create_clicklog_table(self.spark, clicklog_table)
        create_showlog_table(self.spark, showlog_table)

        # Drop the output tables
        util.drop_table(self.hive_context, showlog_output_table)
        util.drop_table(self.hive_context, clicklog_output_table)

        # Run the method to be tested.
        main_clean.clean_logs(cfg, df_persona, df_keywords, log_table_names)

        # Validate the output tables.
        conditions = cfg['pipeline']['main_clean']['conditions']
        df_log = create_raw_log(self.spark)

        # Validate the cleaned clicklog table.
        df_clicklog = util.load_df(self.hive_context, clicklog_output_table)
        self.validate_cleaned_log(df_clicklog, conditions, df_persona, df_keywords, df_log, cfg['pipeline']['main_clean']['did_bucket_num'])

        # Validate the cleaned showlog table.
        df_showlog = util.load_df(self.hive_context, clicklog_output_table)
        self.validate_cleaned_log(df_showlog, conditions, df_persona, df_keywords, df_log, cfg['pipeline']['main_clean']['did_bucket_num'])


    # Testing full data cleaning process for persona, clicklog, and showlog data.
    def test_run (self):
        print('*** Running test_run ***')
        with open('pipeline/config_clean.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        # Create the persona, keywords, clicklog and showlog tables.
        persona_table = cfg['persona_table_name']
        keywords_table = cfg['keywords_table']
        showlog_table = cfg['showlog_table_name']
        clicklog_table = cfg['clicklog_table_name']
        create_persona_table(self.spark, persona_table)
        create_keywords_table(self.spark, keywords_table)
        create_clicklog_table(self.spark, clicklog_table)
        create_showlog_table(self.spark, showlog_table)

        # Drop the output tables
        showlog_output_table = cfg['pipeline']['main_clean']['showlog_output_table']
        clicklog_output_table = cfg['pipeline']['main_clean']['clicklog_output_table']
        persona_output_table = cfg['pipeline']['main_clean']['persona_output_table']
        util.drop_table(self.hive_context, showlog_output_table)
        util.drop_table(self.hive_context, clicklog_output_table)
        util.drop_table(self.hive_context, persona_output_table)

        # Run the method to be tested.
        main_clean.run(self.hive_context, cfg)

        # Validate the output tables.
        conditions = cfg['pipeline']['main_clean']['conditions']
        bucket_num = cfg['pipeline']['main_clean']['did_bucket_num']
        df_keywords = util.load_df(self.hive_context, keywords_table)

        # Validate the cleaned persona table.
        df_persona = util.load_df(self.hive_context, persona_output_table)
        self.validate_clean_persona(df_persona, bucket_num)

        # Validate the cleaned clicklog table.
        df_clicklog = util.load_df(self.hive_context, clicklog_output_table)
        print_df_generator_code(df_clicklog.sort('did'))
        df_log = create_raw_log(self.spark)
        self.validate_cleaned_log(df_clicklog, conditions, df_persona, df_keywords, df_log, bucket_num)

        # Validate the cleaned showlog table.
        df_showlog = util.load_df(self.hive_context, clicklog_output_table)
        print_df_generator_code(df_showlog.sort('did'))
        df_log = create_raw_log(self.spark)
        self.validate_cleaned_log(df_showlog, conditions, df_persona, df_keywords, df_log, bucket_num)


    #========================================
    # Helper methods
    #========================================
    def validate_clean_persona (self, df, bucket_num):
        # The did_bucket column should have been added.
        self.assertTrue('did_bucket' in df.columns)

        # The type for age and gender should have changed to integer.
        schema = df.schema
        self.assertIsInstance(schema['age'].dataType, IntegerType)
        self.assertIsInstance(schema['gender'].dataType, IntegerType)

        # The output should be the same as the generated clean persona dataframe.
        df_clean = create_cleaned_persona(self.spark, bucket_num)

        # Make sure the output isn't missing any rows.
        self.assertEqual(df_clean.subtract(df).count(), 0)

        # Make sure the output doesn't have any extra rows.
        self.assertEqual(df.subtract(df_clean).count(), 0)

    def validate_add_day (self, df):
        # The day column should have been added.
        self.assertTrue('day' in df.columns)

        # Check the entries in the day column using a different method to generate the expected value.
        for row in df.collect():
            print(row['day'])
            self.assertEqual(row['day'], row['action_time'].split()[0])

    def validate_add_did_bucket (self, df, bucket_num):
        # Check that the did_bucket column was added.
        self.assertTrue('did_bucket' in df.columns)

        for row in df.collect():
            self.assertTrue(int(row['did_bucket']) < bucket_num)

    def validate_cleaned_log (self, df, conditions, df_persona, df_keywords, df_log, bucket_num):
        # Verify the column names.
        columns = ['spread_app_id', 'did', 'adv_id', 'media', 'slot_id', 'device_name', 
            'net_type', 'price_model', 'action_time', 'gender', 'age', 
            'keyword', 'keyword_index', 'day', 'did_bucket']
        for name in columns:
            self.assertTrue(name in df.columns)

        # Verify the number of rows.
        # The raw log count has one entry that will be filtered out so adjusted accordingly.
        self.assertEqual(df.count(), df_log.count() - 1)

        # Helper method for verifying table joins.
        def assert_row_value (row, df_match, field_name, join_field):
            self.assertEqual(row[field_name], df_match.filter(col(join_field) == row[join_field]).collect()[0][field_name])

        # Check the row values.
        for row in df.collect():
            self.assertTrue(row['slot_id'] in conditions['new_slot_id_list'])
            self.assertEqual(row['day'], row['action_time'].split()[0])
            self.assertTrue(int(row['did_bucket']) < bucket_num)
            assert_row_value(row, df_persona, 'gender', 'did')
            assert_row_value(row, df_persona, 'age', 'did')
            assert_row_value(row, df_keywords, 'keyword', 'spread_app_id')
            assert_row_value(row, df_keywords, 'keyword_index', 'spread_app_id')
            assert_row_value(row, df_log, 'adv_id', 'did')
            assert_row_value(row, df_log, 'media', 'did')
            assert_row_value(row, df_log, 'slot_id', 'did')
            assert_row_value(row, df_log, 'device_name', 'did')
            assert_row_value(row, df_log, 'net_type', 'did')
            assert_row_value(row, df_log, 'price_model', 'did')
            assert_row_value(row, df_log, 'action_time', 'did')


# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()