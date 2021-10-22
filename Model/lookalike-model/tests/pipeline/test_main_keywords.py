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
from lookalike_model.pipeline import main_keywords, util
from data_generator import *

class TestMainKeywords(unittest.TestCase):

    def setUp (self):
        # Set the log level.
        sc = SparkContext.getOrCreate()
        sc.setLogLevel('ERROR')

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.hive_context = HiveContext(sc)

    def test_run(self):
        print('*** Running test_run ***')
        with open('pipeline/config_keywords.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        util.resolve_placeholder(cfg)

        # Create the input tables.
        cfg_clean = cfg['pipeline']['main_clean']
        showlog_table = cfg['showlog_table_name']
        keywords_mapping_table = cfg['keywords_table']
        create_keywords_mapping = cfg_clean['create_keywords']
        create_keywords_table(self.spark, keywords_mapping_table)
        create_keywords_showlog_table(self.spark, showlog_table)

        # Drop the output tables.
        cfg_keywords = cfg['pipeline']['main_keywords']
        keyword_threshold = cfg_keywords['keyword_threshold']
        effective_keywords_table = cfg_keywords['keyword_output_table']
        util.drop_table(self.hive_context, effective_keywords_table)

        start_date, end_date, load_minutes = util.load_batch_config(cfg)

        main_keywords.run(self.hive_context, showlog_table, keywords_mapping_table, create_keywords_mapping, 
            start_date, end_date, load_minutes, keyword_threshold, effective_keywords_table)

        df_keywords = util.load_df(self.hive_context, effective_keywords_table)

        self.validate_effective_keywords(df_keywords)


    def validate_effective_keywords(self, df):
        # Check the column names.
        self.assertTrue('keyword' in df.columns)

        # Expected keywords in the dataframe.
        keywords_match = ['education', 'travel', 'game-avg']

        # Check number of rows.
        self.assertEqual(df.count(), len(keywords_match))

        df.show()

        # Check the values of the rows.
        # keywords = df.agg(collect_list('keyword')).collect()
        # for value in keywords_match:
        #     self.assertTrue(value in keywords)
        for row in df.collect():
            self.assertIn(row['keyword'], keywords_match)

# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()
