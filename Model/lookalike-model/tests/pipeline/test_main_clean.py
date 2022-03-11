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
from pyspark.sql.functions import col, udf, collect_set
from pyspark.sql.types import IntegerType, BooleanType
from lookalike_model.pipeline import main_clean, util
from data_generator import *


class TestMainClean(unittest.TestCase):

    def setUp(self):
        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')

    # Testing the method that tests user uniqueness and removes users with conflicting age/gender.

    def compare_list(self, list1, list2):
        if len(list1) != len(list2):
            return False

        def _key(x): return '-'.join([str(_) for _ in x])
        return sorted(list1, key=_key) == sorted(list2, key=_key)

    def _test_clean_persona(self):
        print('*** Running test_clean_persona ***')

        data = [
            ('0000001', 0, 0),
            ('0000001', 0, 0),  # duplicate entry, duplicates will be removed
            ('0000002', 1, 1),
            ('0000003', 2, 2),
            ('0000003', 2, 3),  # duplicate entry, duplicates will be removed
        ]
        schema = StructType([
            StructField("aid", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", StringType(), True)
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        df = main_clean.clean_persona(df, 1).select('aid', 'gender', 'age')

        expected_output = [
            ('0000001', 0, 0),
            ('0000002', 1, 1)
        ]

        print("Original DataFrame df:")
        print(df.show(100, False))

        print("Expected DataFrame df_expected:")
        print(expected_output)
        self.assertTrue(self.compare_list(df.collect(), expected_output))

    # Tests the method that adds the 'day' column with just the date from the action_time column.
    def _test_add_day(self):
        print('*** Running test_add_day ***')
        data = [
            ('0000001', '2022-02-19 12:34:56.78'),
            ('0000002', '2022-02-20 12:34:56.78')
        ]
        schema = StructType([
            StructField("aid", StringType(), True),
            StructField("action_time", StringType(), True)
        ])
        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        # Run the method to be tested.
        df = main_clean.add_day(df)
        print(df.show(100, False))

        expected_output = [
            ('0000001', '2022-02-19 12:34:56.78', '2022-02-19'),
            ('0000002', '2022-02-20 12:34:56.78', '2022-02-20')
        ]

        schema_expected = StructType([
            StructField("aid", StringType(), True),
            StructField("action_time", StringType(), True),
            StructField("day", StringType(), True),
        ])
        df_expected = self.spark.createDataFrame(self.spark.sparkContext.parallelize(expected_output), schema_expected)
        df_expected = main_clean.add_day(df_expected)
        print("Original DataFrame df:")
        print(df.show(100, False))
        print("Expected DataFrame df_expected:")
        print(df_expected.show(100, False))
        result = sorted(df.collect()) == sorted(df_expected.collect())
        print result

    # Testing the addition of the 'aid_bucket' column with a hash mod bucket_num value.
    def _test_add_aid_bucket(self):
        print('*** Running test_add_aid_bucket ***')
        # Input
        data = [
            ('0000001', 0, 0),
            ('0000002', 1, 1),
            ('0000003', 2, 2),
        ]
        schema = StructType([
            StructField("aid", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", StringType(), True)
        ]
        )
        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        # Run the method to be tested.
        aid_bucket_num = 4
        df = main_clean.add_aid_bucket(df, aid_bucket_num)
        expected_output = [
            ('0000001', 0, 0),
            ('0000002', 1, 1),
            ('0000003', 2, 2)
        ]
        df_expected = add_aid_bucket(self.spark.createDataFrame(self.spark.sparkContext.parallelize(expected_output), schema), aid_bucket_num)
        print("Original DataFrame df:")
        print(df.show(100, False))
        print("Expected DataFrame df_expected:")
        print(df_expected.show(100, False))
        result = sorted(df.collect()) == sorted(df_expected.collect())
        print result

    # Testing the method that joins the log rows with the user persona, keyword, and media category.
    def _test_clean_batched_log(self):
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
        aid_bucket_num = 4
        df = main_clean.clean_batched_log(df_log, df_persona, df_keywords, aid_bucket_num)
        print(df.sort('aid').show(100, False))

        # Validate the output.
        self.validate_cleaned_log(df, df_persona, conditions, df_keywords, df_log, aid_bucket_num)

    # Testing data look up and cleaning process for clicklog and showlog data.
    def _test_clean_logs(self):
        print('*** Running test_clean_logs ***')
        with open('config_clean.yml', 'r') as ymlfile:
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
        print(df_clicklog.sort('action_time').show(100, False))
        self.validate_cleaned_log(df_clicklog, conditions, df_persona, df_keywords, df_log, cfg['pipeline']['main_clean']['aid_bucket_num'])

        # Validate the cleaned showlog table.
        df_showlog = util.load_df(self.hive_context, clicklog_output_table)
        self.validate_cleaned_log(df_showlog, conditions, df_persona, df_keywords, df_log, cfg['pipeline']['main_clean']['aid_bucket_num'])

    # Testing full data cleaning process for persona, clicklog, and showlog data.

    def _test_run(self):
        print('*** Running test_run ***')
        with open('config_clean.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        # Create the persona, keywords, clicklog and showlog tables.
        persona_table = cfg['persona_table_name']
        keywords_table = cfg['keywords_table']
        showlog_table = cfg['showlog_table_name']
        clicklog_table = cfg['clicklog_table_name']
        effective_keywords_table = cfg['pipeline']['main_keywords']['keyword_output_table']
        create_persona_table(self.spark, persona_table)
        create_keywords_table(self.spark, keywords_table)
        create_clicklog_table(self.spark, clicklog_table)
        create_showlog_table(self.spark, showlog_table)
        create_effective_keywords_table(self.spark, effective_keywords_table)

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
        bucket_num = cfg['pipeline']['main_clean']['aid_bucket_num']
        df_keywords = util.load_df(self.hive_context, keywords_table)

        # run() does filtering on the effective keywords so we need to filter
        # the raw logs with the spread app ids when validating the output.
        effective_spread_app_ids = ['C000', 'C001', 'C002', 'C003', 'C004', 'C010', 'C011', 'C012', 'C013', 'C014', ]
        df_log = create_raw_log(self.spark)
        df_log = self.filter_spread_app_ids(df_log, effective_spread_app_ids)

        # Validate the cleaned persona table.
        df_persona = util.load_df(self.hive_context, persona_output_table)
        self.validate_clean_persona(df_persona, bucket_num)

        # Validate the cleaned clicklog table.
        df_clicklog = util.load_df(self.hive_context, clicklog_output_table)
        self.validate_cleaned_log(df_clicklog, conditions, df_persona, df_keywords, df_log, bucket_num)
        print_df_generator_code(df_clicklog.sort('aid'))

        # Validate the cleaned showlog table.
        df_showlog = util.load_df(self.hive_context, clicklog_output_table)
        self.validate_cleaned_log(df_showlog, conditions, df_persona, df_keywords, df_log, bucket_num)
        print_df_generator_code(df_showlog.sort('aid'))

    def filter_spread_app_ids(self, df, spread_app_ids):
        # User defined function to return if the keyword is in the inclusion set.
        _udf = udf(lambda x: x in spread_app_ids, BooleanType())

        # Return the filtered dataframe.
        return df.filter(_udf(col('spread_app_id')))

    # ========================================
    # Helper methods
    # ========================================
    def validate_cleaned_log(self, df, conditions, df_persona, df_keywords, df_log, bucket_num):
        # Verify the column names.
        columns = ['spread_app_id', 'aid', 'adv_id', 'media', 'slot_id', 'device_name',
                   'net_type', 'price_model', 'action_time', 'gender', 'age',
                   'gender_index', 'keyword', 'day', 'aid_bucket']
        for name in columns:
            self.assertTrue(name in df.columns)

        # Verify the number of rows.
        # The raw log count has one entry that will be filtered out so adjusted accordingly.
        self.assertEqual(df.count(), df_log.count() - 1)

        # Helper method for verifying table joins.
        def assert_row_value(row, df_match, field_name, join_field):
            self.assertEqual(row[field_name], df_match.filter(col(join_field) == row[join_field]).collect()[0][field_name])

        # Check the row values.
        for row in df.collect():
            self.assertTrue(row['slot_id'] in conditions['new_slot_id_list'])
            self.assertEqual(row['day'], row['action_time'].split()[0])
            self.assertTrue(int(row['aid_bucket']) < bucket_num)
            assert_row_value(row, df_persona, 'gender', 'aid')
            assert_row_value(row, df_persona, 'age', 'aid')
            assert_row_value(row, df_keywords, 'keyword', 'spread_app_id')
            assert_row_value(row, df_keywords, 'keyword_index', 'spread_app_id')
            assert_row_value(row, df_log, 'adv_id', 'aid')
            assert_row_value(row, df_log, 'media', 'aid')
            assert_row_value(row, df_log, 'slot_id', 'aid')
            assert_row_value(row, df_log, 'device_name', 'aid')
            assert_row_value(row, df_log, 'net_type', 'aid')
            assert_row_value(row, df_log, 'price_model', 'aid')
            assert_row_value(row, df_log, 'action_time', 'aid')


# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()
