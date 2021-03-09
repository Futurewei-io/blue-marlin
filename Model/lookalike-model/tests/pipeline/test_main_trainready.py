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
import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from lookalike_model.pipeline import main_trainready, util
from data_generator import *

class TestMainTrainReady (unittest.TestCase):

    def setUp (self):
        # Set the log level.
        sc = SparkContext.getOrCreate()
        sc.setLogLevel('ERROR')

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.hive_context = HiveContext(sc)

    def test_generate_trainready (self):
        print('*** Running test_generate_trainready test ***')

        # Load the configuration data.
        with open('pipeline/config_trainready.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        # Load the training parameters.
        did_bucket_num = cfg['pipeline']['main_clean']['did_bucket_num']
        interval_time_in_seconds = cfg['pipeline']['main_logs']['interval_time_in_seconds']
        log_table = cfg['pipeline']['main_logs']['logs_output_table_name']
        trainready_table = cfg['pipeline']['main_trainready']['trainready_output_table']

        # Create the input data table.
        create_unified_log_table(self.spark, log_table)

        # Clear the output from any previous runs.
        util.drop_table(self.hive_context, trainready_table)

        # Run the method to be tested.
        main_trainready.generate_trainready(self.hive_context, util.load_batch_config(cfg),
            interval_time_in_seconds, log_table, trainready_table, did_bucket_num)

        # Validate the output.
        df = util.load_df(self.hive_context, trainready_table)
        self.validate_trainready_output(df, create_unified_log(self.spark))


    def test_run (self):
        # Load the configuration data.
        with open('pipeline/config_trainready.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        # Create the input data table.
        log_table = cfg['pipeline']['main_logs']['logs_output_table_name']
        create_unified_log_table(self.spark, log_table)

        # Clear the output from any previous runs.
        trainready_table = cfg['pipeline']['main_trainready']['trainready_output_table']
        util.drop_table(self.hive_context, trainready_table)

        # Run the method to be tested.
        main_trainready.run(self.hive_context, cfg)

        # Validate the output.
        df = util.load_df(self.hive_context, trainready_table)
        print(df.sort('did').show(100, False))
        print_df_generator_code(df.sort('did'))
        self.validate_trainready_output(df, create_unified_log(self.spark))

    def validate_trainready_output (self, df, df_input_logs):
        # Parses a list of list of ints or dicts of ints into a list of dicts/sets.
        def parse_int_field (values):
            result = []
            for value in values:
                # Evaluate the value as a set or map.
                # Does not work for strings since there are no quotes.
                result.append(eval('{' + value + '}'))
            return result

        # Parses a list of list of strings into a list of sets of strings.
        def parse_string_field (values):
            result = []
            for value in values:
                temp = set()
                for val in value.split(','):
                    temp.add(val)
                result.append(temp)
            return result

        # Verify the column names.
        columns = {'did', 'did_index', 'did_bucket', 'interval_keywords', 
            'gender', 'age', 'interval_starting_time',
            'kwi', 'kwi_show_counts', 'kwi_click_counts'}
        for name in columns:
            self.assertTrue(name in df.columns)

        # Check that the number of rows is the same of the number of
        # unique did's.
        self.assertEqual(df.count(), df_input_logs.select('did').distinct().count())

        seen_did = set()
        for did in [ i['did'] for i in df_input_logs.select('did').distinct().collect() ]:
            # Get the rows of the input and output data set for this did.
            seen_did.add(did)
            df_did = df.where(col('did') == did)
            df_input_did = df_input_logs.where(col('did') == did)

            # Verify that the output has only one row for this did.
            self.assertEqual(df_did.count(), 1)
            print(df_did.show(1, False))
            print(df_input_did.show(100, False))

            # Verify the non-aggregate columns.
            df_did_row = df_did.collect()[0]
            for column in ['did_bucket', 'age', 'gender']:
                self.assertEqual(df_did_row[column], 
                    df_input_did.collect()[0][column])

            # Parse the show and click count fields.
            show_counts = parse_int_field(df_did_row['kwi_show_counts'])
            click_counts = parse_int_field(df_did_row['kwi_click_counts'])

            # Parse the keyword fields.
            keywords = parse_string_field(df_did_row['interval_keywords'])
            kwi = parse_int_field(df_did_row['kwi'])

            # Verify the aggregate columns by start time.
            seen_start_times = set()
            for start_time in [ int(i['interval_starting_time']) for i in df_input_did.select('interval_starting_time').distinct().collect() ]:
                seen_start_times.add(start_time)

                # Find the index of the start time.
                index = -1
                for i, time in enumerate(df_did_row['interval_starting_time']):
                    if int(time) == start_time:
                        index = i
                # Fail if the start time is not found.
                self.assertNotEqual(index, -1)

                # Get the rows of the input dataframe for this start time.
                df_st = df_input_did.where(col('interval_starting_time') == str(start_time))

                # Iterate through the keyword indexes for this start time.
                keyword_indexes = df_st.select('keyword_index').distinct().collect()
                seen_kwi = set()
                seen_keywords = set()
                for keyword_index in [ int(i['keyword_index']) for i in keyword_indexes ]:
                    # Get the rows of the input dataframe with this did, start time, and keyword index.
                    seen_kwi.add(keyword_index)
                    df_kw = df_st.where(col('keyword_index') == keyword_index)
                    # print(df_kw.show(10, False))

                    # Validate the keyword and keyword index.
                    self.assertTrue(keyword_index in kwi[index])
                    for row in df_kw.collect():
                        self.assertTrue(int(row['keyword_index']) in kwi[index])
                        self.assertTrue(row['keyword'] in keywords[index])
                        seen_keywords.add(row['keyword'])

                    # Get the show and click count this this did, start time, and keyword index.
                    show_count = df_kw.where(col('is_click') == 0).count()
                    click_count = df_kw.where(col('is_click') == 1).count()

                    # Validate the click and show counts.
                    self.assertEqual(show_counts[index][keyword_index], show_count)
                    self.assertEqual(click_counts[index][keyword_index], click_count)

                # Check for any additional keyword indices that should not be there.
                self.assertEqual(len(show_counts[index]), len(seen_kwi))
                for key in show_counts[index].keys():
                    self.assertTrue(key in seen_kwi)
                self.assertEqual(len(click_counts[index]), len(seen_kwi))
                for key in click_counts[index].keys():
                    self.assertTrue(key in seen_kwi)
                self.assertEqual(len(kwi[index]), len(seen_kwi))
                for i in kwi[index]:
                    self.assertTrue(i in seen_kwi)
                self.assertEqual(len(keywords[index]), len(seen_keywords))
                for i in keywords[index]:
                    self.assertTrue(i in seen_keywords)

            # Check for any additional start times that should not be there.
            self.assertEqual(len(df_did_row['interval_starting_time']), len(seen_start_times))
            for i in df_did_row['interval_starting_time']:
                self.assertTrue(int(i) in seen_start_times)

        # Check for any did's that shouldn't be there.
        output_did = [ i['did'] for i in df_input_logs.select('did').distinct().collect()]
        self.assertEqual(len(output_did), len(seen_did))
        for i in output_did:
            self.assertTrue(i in seen_did)

# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()
