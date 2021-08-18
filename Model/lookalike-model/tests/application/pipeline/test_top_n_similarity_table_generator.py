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
from pyspark.sql import HiveContext, Row, SparkSession
from pyspark.sql.functions import col, udf, collect_set
from pyspark.sql.types import IntegerType, BooleanType,StructType,StructField, StringType
from lookalike_model.pipeline import main_clean, util
from lookalike_model.pipeline.util import write_to_table
from lookalike_model.application.pipeline import top_n_similarity_table_generator
import random
import string

'''
spark-submit --master yarn --num-executors 2 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict test_top_n_similarity_table_generator.py
'''


def random_string_generator(str_size):
    PREFIX = 'loolalike_application_unittest_'
    return PREFIX+''.join(random.choice(string.ascii_letters) for _ in range(str_size))


class TestMainClean(unittest.TestCase):

    def setUp(self):
        # Set the log level.
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel('ERROR')

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.hive_context = HiveContext(self.sc)

    def drop_table(self, table_name):
        self.hive_context.sql('DROP TABLE {}'.format(table_name))

    def run_top_n_similarity_table_generator(self, cfg, _input):
        df_input = self.hive_context.createDataFrame(_input, ['did', 'score_vector', 'c1', 'did_bucket', 'alpha_did_bucket'])
        write_to_table(df_input, cfg['score_vector_rebucketing']['score_vector_alpha_table'])
        top_n_similarity_table_generator.run(self.sc, self.hive_context, cfg)
        result = self.hive_context.sql('SELECT did,top_n_similar_user,did_bucket FROM {}'.format(cfg['top_n_similarity']['similarity_table'])).collect()
       
        # Remove tmp tables
        self.drop_table(cfg['score_vector_rebucketing']['score_vector_alpha_table'])
        self.drop_table(cfg['top_n_similarity']['similarity_table'])

        return result

    def test_run_1(self):
        cfg = {
            'score_vector': {'did_bucket_size': 2},

            'score_vector_rebucketing': {
                'did_bucket_size': 2,
                'did_bucket_step': 2,
                'alpha_did_bucket_size': 20,
                'score_vector_alpha_table': random_string_generator(10)},

            'top_n_similarity': {'did_bucket_step': 1,
                                 'alpha_did_bucket_step': 10,
                                 'top_n': 10,
                                 'similarity_table': random_string_generator(10)}
        }

        _input = [('1', [0.1, 0.8, 0.9], 1.46, 0, 0), ('2', [0.1, 0.8, 0.9], 1.46, 0, 0)]
        _expected_output = [
            ('1', [{'did': '1', 'score': 0.05777484551072121}, {'did': '2', 'score': 0.05777484551072121}], 0),
            ('1', [{'did': '1', 'score': 0.05777484551072121}, {'did': '2', 'score': 0.05777484551072121}], 0)
        ]

        result = self.run_top_n_similarity_table_generator(cfg, _input)
        
        print(_expected_output)
        print(result)

        # Add assert


# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()
