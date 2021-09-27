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
from pyspark.sql.types import IntegerType, BooleanType, StructType, StructField, StringType, StructType, ArrayType, FloatType
from lookalike_model.pipeline import main_clean, util
from lookalike_model.pipeline.util import write_to_table
from lookalike_model.application.pipeline import top_n_similarity_table_generator
import random
import string

'''
spark-submit --master yarn --num-executors 2 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict test_top_n_similarity_table_generator_1.py
'''


def random_string_generator(str_size):
    PREFIX = 'lookalike_application_unittest_'
    return PREFIX+''.join(random.choice(string.ascii_letters) for _ in range(str_size))


class TestMainClean(unittest.TestCase):

    def setUp(self):
        # Set the log level.

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').config(
            "spark.archives",  # 'spark.yarn.dist.archives' in YARN.
            # "spark.yarn.dist.archives",  # 'spark.yarn.dist.archives' in YARN.
            "lookalike-application-python-venv.tar.gz#environment").enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')

    def drop_table(self, table_name):
        self.spark.sql('DROP TABLE {}'.format(table_name))

    def create_matrix_table(self, data, table_name):
        schema = StructType([
            StructField("did_list", ArrayType(StringType(), False)),
            StructField("score_matrix", ArrayType(ArrayType(FloatType(), False)), False),
            StructField("c1_list", ArrayType(FloatType(), False)),
            StructField("did_bucket", IntegerType(), False)
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        util.write_to_table(df, table_name)

    def run_top_n_similarity_table_generator(self, cfg, _input):
        self.create_matrix_table(_input, cfg['score_matrix_table']['score_matrix_table'])
        top_n_similarity_table_generator.run(self.spark, cfg)
        result = self.spark.sql('SELECT did,top_n_similar_user,did_bucket FROM {}'.format(cfg['top_n_similarity']['similarity_table']))
        return result

    def compare_output_and_expected_and_cleanup(self, cfg, test_name, df_output, _expected_output):
        elements_type = StructType([StructField('did', StringType(), False), StructField('score', FloatType(), False)])
        _schema = StructType([StructField('did', StringType(), False), StructField('top_n_similar_user',
                                                                                   ArrayType(elements_type), False), StructField('did_bucket', IntegerType(), False)])
        df_expected_output = self.spark.createDataFrame(_expected_output, _schema)

        df_output = df_output.sort('did')
        df_expected_output = df_expected_output.sort('did')

        print('Test name : {}'.format(test_name))

        print('Expected')
        df_expected_output.show(10, False)

        print('Output')
        df_output.show(10, False)

        diff = df_output.subtract(df_expected_output)
        print('Difference')
        diff.show(10, False)
        diff_count = diff.count()

        # Clean up: Remove tmp tables
        self.drop_table(cfg['score_matrix_table']['score_matrix_table'])
        self.drop_table(cfg['top_n_similarity']['similarity_table'])

        return diff_count == 0

    def test_run_1(self):
        cfg = {
            'score_matrix_table': {
                'did_bucket_size': 2,
                'did_bucket_step': 2,
                'score_matrix_table': random_string_generator(10)
            },

            'top_n_similarity': {'did_bucket_size': 2,
                                 'did_bucket_step': 2,
                                 'cross_bucket_size': 2,
                                 'top_n': 10,
                                 'similarity_table': random_string_generator(10)}
        }

        _input = [(['1', '2'], [[0.1, 0.8, 0.9], [0.1, 0.8, 0.9]], [1.46, 1.46], 0), (['3', '4'], [[0.1, 0.8, 0.9], [0.1, 0.8, 0.9]], [1.46, 1.46], 1)]
        _expected_output = [
            ('1', [Row(did='1', score=1.7316996), Row(did='2', score=1.7316996), Row(did='3', score=1.7316996), Row(did='4', score=1.7316996)], 0),
            ('2', [Row(did='1', score=1.7316996), Row(did='2', score=1.7316996), Row(did='3', score=1.7316996), Row(did='4', score=1.7316996)], 0),
            ('3', [Row(did='1', score=1.7316996), Row(did='2', score=1.7316996), Row(did='3', score=1.7316996), Row(did='4', score=1.7316996)], 1),
            ('4', [Row(did='1', score=1.7316996), Row(did='2', score=1.7316996), Row(did='3', score=1.7316996), Row(did='4', score=1.7316996)], 1),
        ]

        df_output = self.run_top_n_similarity_table_generator(cfg, _input)
        are_equal = self.compare_output_and_expected_and_cleanup(cfg, 'test_run_1', df_output, _expected_output)
        self.assertTrue(are_equal)

    def test_run_2(self):
        cfg = {
            'score_matrix_table': {
                'did_bucket_size': 2,
                'did_bucket_step': 2,
                'score_matrix_table': random_string_generator(10)
            },

            'top_n_similarity': {'did_bucket_size': 2,
                                 'did_bucket_step': 2,
                                 'cross_bucket_size': 2,
                                 'top_n': 10,
                                 'similarity_table': random_string_generator(10)}
        }

        _input = [(['1', '2'], [[0.1, 0.8, 0.9], [0.1, 0.1, 0.1]], [1.46, 0.03], 0), (['3', '4'], [[0.1, 0.8, 0.9], [0.1, 0.2, 0.3]], [1.46, 0.14], 1)]
        _expected_output = [
            ('1', [Row(did='1', score=1.7316996), Row(did='3', score=1.7316996), Row(did='4', score=0.8835226), Row(did='2', score=0.6690362)], 0),
            ('2', [Row(did='2', score=1.7320508), Row(did='4', score=1.5084441), Row(did='3', score=0.6690362), Row(did='1', score=0.6690362)], 0),
            ('3', [Row(did='1', score=1.7316996), Row(did='3', score=1.7316996), Row(did='4', score=0.8835226), Row(did='2', score=0.6690362)], 1),
            ('4', [Row(did='4', score=1.7320508), Row(did='2', score=1.5084441), Row(did='3', score=0.8835226), Row(did='1', score=0.8835226)], 1),
        ]

        df_output = self.run_top_n_similarity_table_generator(cfg, _input)
        are_equal = self.compare_output_and_expected_and_cleanup(cfg, 'test_run_2', df_output, _expected_output)
        self.assertTrue(are_equal)

    def test_run_3(self):
        cfg = {
            'score_matrix_table': {
                'did_bucket_size': 2,
                'did_bucket_step': 2,
                'score_matrix_table': random_string_generator(10)
            },

            'top_n_similarity': {'did_bucket_size': 2,
                                 'did_bucket_step': 2,
                                 'cross_bucket_size': 2,
                                 'top_n': 2,
                                 'similarity_table': random_string_generator(10)}
        }

        _input = [(['1', '2'], [[0.1, 0.8, 0.9], [0.1, 0.1, 0.1]], [1.46, 0.03], 0), (['3', '4'], [[0.1, 0.8, 0.9], [0.1, 0.2, 0.3]], [1.46, 0.14], 1)]
        _expected_output = [
            ('1', [Row(did='1', score=1.7316996), Row(did='3', score=1.7316996)], 0),
            ('2', [Row(did='2', score=1.7320508), Row(did='4', score=1.5084441)], 0),
            ('3', [Row(did='1', score=1.7316996), Row(did='3', score=1.7316996)], 1),
            ('4', [Row(did='4', score=1.7320508), Row(did='2', score=1.5084441)], 1),
        ]

        df_output = self.run_top_n_similarity_table_generator(cfg, _input)
        are_equal = self.compare_output_and_expected_and_cleanup(cfg, 'test_run_3', df_output, _expected_output)
        self.assertTrue(are_equal)


# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()
