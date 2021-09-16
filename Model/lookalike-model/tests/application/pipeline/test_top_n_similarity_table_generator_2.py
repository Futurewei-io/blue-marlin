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
from pyspark.sql.types import DataType, StringType, StructField, StructType, FloatType, IntegerType, ArrayType, MapType
from lookalike_model.application.pipeline import util, top_n_similarity_table_generator


class TestTopNSimilarityTableGenerator(unittest.TestCase):

    def setUp (self):
        # Set the log level.
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel('ERROR')

        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.hive_context = HiveContext(self.sc)

    def test_run(self):
        print('*** Running TestTopNSimilarityTableGenerator.test_run ***')

        # Load the test configuration.
        with open('application/pipeline/config_top_n_similarity.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)

        # Get the names of the input and output tables.
        # alpha_table = cfg['score_vector_rebucketing']['score_vector_alpha_table']
        matrix_table = cfg['score_matrix_table']['score_matrix_table']
        top_n_table = cfg['top_n_similarity']['similarity_table']

        # Create the input table.
        # self.create_alpha_table(alpha_table)
        self.create_matrix_table(matrix_table)

        # Run the function being tested.
        top_n_similarity_table_generator.run(self.spark, self.hive_context, cfg)

        # Load the output of the function.
        command = """select * from {} order by did""".format(top_n_table)
        df = self.hive_context.sql(command)
        df.show()

        # Validate the output.
        self.validate_similarity_table(df, True)

    def test_run2(self):
        print('*** Running TestTopNSimilarityTableGenerator.test_run2 ***')

        # Load the test configuration.
        with open('application/pipeline/config_top_n_similarity.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)
        cfg['top_n_similarity']['did_bucket_size'] = 2
        cfg['top_n_similarity']['cross_bucket_size'] = 2

        # Get the names of the input and output tables.
        # alpha_table = cfg['score_vector_rebucketing']['score_vector_alpha_table']
        matrix_table = cfg['score_matrix_table']['score_matrix_table']
        top_n_table = cfg['top_n_similarity']['similarity_table']

        # Create the input table.
        # self.create_alpha_table(alpha_table)
        self.create_matrix_table2(matrix_table)

        # Run the function being tested.
        top_n_similarity_table_generator.run(self.spark, self.hive_context, cfg)

        # Load the output of the function.
        command = """select * from {} order by did""".format(top_n_table)
        df = self.hive_context.sql(command)
        df.show()

        # Validate the output.
        self.validate_similarity_table(df)

    def test_run3(self):
        print('*** Running TestTopNSimilarityTableGenerator.test_run3 ***')

        # Load the test configuration.
        with open('application/pipeline/config_top_n_similarity.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)
        cfg['top_n_similarity']['did_bucket_size'] = 2
        cfg['top_n_similarity']['did_bucket_step'] = 2
        cfg['top_n_similarity']['cross_bucket_size'] = 2

        # Get the names of the input and output tables.
        # alpha_table = cfg['score_vector_rebucketing']['score_vector_alpha_table']
        matrix_table = cfg['score_matrix_table']['score_matrix_table']
        top_n_table = cfg['top_n_similarity']['similarity_table']

        # Create the input table.
        # self.create_alpha_table(alpha_table)
        self.create_matrix_table2(matrix_table)

        # Run the function being tested.
        top_n_similarity_table_generator.run(self.spark, self.hive_context, cfg)

        # Load the output of the function.
        command = """select * from {} order by did""".format(top_n_table)
        df = self.hive_context.sql(command)
        df.show()

        # Validate the output.
        self.validate_similarity_table(df)

    def test_run4(self):
        print('*** Running TestTopNSimilarityTableGenerator.test_run4 ***')

        # Load the test configuration.
        with open('application/pipeline/config_top_n_similarity.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)
        cfg['top_n_similarity']['did_bucket_size'] = 2
        cfg['top_n_similarity']['did_bucket_step'] = 2
        cfg['top_n_similarity']['cross_bucket_size'] = 2
        cfg['top_n_similarity']['cross_bucket_step'] = 2

        # Get the names of the input and output tables.
        # alpha_table = cfg['score_vector_rebucketing']['score_vector_alpha_table']
        matrix_table = cfg['score_matrix_table']['score_matrix_table']
        top_n_table = cfg['top_n_similarity']['similarity_table']

        # Create the input table.
        # self.create_alpha_table(alpha_table)
        self.create_matrix_table2(matrix_table)

        # Run the function being tested.
        top_n_similarity_table_generator.run(self.spark, self.hive_context, cfg)

        # Load the output of the function.
        command = """select * from {} order by did""".format(top_n_table)
        df = self.hive_context.sql(command)
        df.show()

        # Validate the output.
        self.validate_similarity_table(df)

    def create_matrix_table(self, table_name):
        data = [
            (['0000001', '0000002', '0000003', '0000004'], 
            [[0.1, 0.8, 0.9], [0.1, 0.1, 0.1], [0.1, 0.8, 0.9], [0.1, 0.2, 0.3]], 
            [1.46, 0.03, 1.46, 0.14], 
            0)
        ]

        schema = StructType([
            StructField("did_list", ArrayType(StringType(), True)),
            StructField("score_matrix", ArrayType(ArrayType(FloatType(), True)), True),
            StructField("c1_list", ArrayType(FloatType(), True)),
            StructField("did_bucket", IntegerType(), True)
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        util.write_to_table(df, table_name)

    def create_matrix_table2(self, table_name):
        data = [
            (['0000001', '0000002'], [[0.1, 0.8, 0.9], [0.1, 0.1, 0.1]], [1.46, 0.03], 0),
            (['0000003', '0000004'], [[0.1, 0.8, 0.9], [0.1, 0.2, 0.3]], [1.46, 0.14], 1)
        ]

        schema = StructType([
            StructField("did_list", ArrayType(StringType(), True)),
            StructField("score_matrix", ArrayType(ArrayType(FloatType(), True)), True),
            StructField("c1_list", ArrayType(FloatType(), True)),
            StructField("did_bucket", IntegerType(), True)
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        util.write_to_table(df, table_name)

    def create_alpha_table (self, table_name):
        data = [
            ('0000001', [0.1, 0.8, 0.9], 1.46, 0, 0),
            ('0000002', [0.1, 0.1, 0.1], 0.03, 0, 1),
            ('0000003', [0.1, 0.8, 0.9], 1.46, 1, 2),
            ('0000004', [0.1, 0.2, 0.3], 0.14, 1, 3),
        ]

        schema = StructType([
            StructField("did", StringType(), True),
            StructField("score_vector", ArrayType(FloatType(), True), True),
            StructField("c1", FloatType(), True),
            StructField("did_bucket", IntegerType(), True),
            StructField("alpha_did_bucket", IntegerType(), True)
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        util.write_to_table(df, table_name)

    def validate_similarity_table (self, df, same_bucket=False):
        later_bucket = 1
        if same_bucket:
            later_bucket = 0
        data = [
            ('0000001', [{'did':'0000001', 'score':1.73205081}, {'did':'0000003', 'score':1.73205081}, {'did':'0000004', 'score':0.88532267}, {'did':'0000002', 'score':0.66903623}], 0),
            ('0000002', [{'did':'0000002', 'score':1.73205081}, {'did':'0000004', 'score':1.50844401}, {'did':'0000001', 'score':0.66903623}, {'did':'0000003', 'score':0.66903623}], 0),
            ('0000003', [{'did':'0000001', 'score':1.73205081}, {'did':'0000003', 'score':1.73205081}, {'did':'0000004', 'score':0.88532267}, {'did':'0000002', 'score':0.66903623}], later_bucket),
            ('0000004', [{'did':'0000004', 'score':1.73205081}, {'did':'0000002', 'score':1.50844401}, {'did':'0000001', 'score':0.88532267}, {'did':'0000003', 'score':0.88532267}], later_bucket)
        ]

        # data = [
        #     (['0000001', '0000002', '0000003', '0000004'], 
        #     [[{'did':'0000001', 'score':1.73205081}, {'did':'0000003', 'score':1.73205081}, {'did':'0000004', 'score':0.88532267}, {'did':'0000002', 'score':0.66903623}], 
        #      [{'did':'0000002', 'score':1.73205081}, {'did':'0000004', 'score':1.50844401}, {'did':'0000001', 'score':0.66903623}, {'did':'0000003', 'score':0.66903623}], 
        #      [{'did':'0000001', 'score':1.73205081}, {'did':'0000003', 'score':1.73205081}, {'did':'0000004', 'score':0.88532267}, {'did':'0000002', 'score':0.66903623}], 
        #      [{'did':'0000004', 'score':1.73205081}, {'did':'0000002', 'score':1.50844401}, {'did':'0000001', 'score':0.88532267}, {'did':'0000003', 'score':0.88532267}]], 
        #     0)
        # ]

        schema = StructType([
            StructField("did", StringType(), True),
            StructField("top_n_similar_user", ArrayType(MapType(StringType(), StringType(), True), True)),
            StructField("did_bucket", IntegerType(), True)
        ])

        df_ref = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        # util.write_to_table(df, table_name)

        # Verify the column names.
        self.assertEqual(len(df.columns), len(df_ref.columns))
        for name in df.columns:
            self.assertIn(name, df_ref.columns)

        # Verify the number of rows.
        self.assertEqual(df.count(), df_ref.count())

        # Check the row values.
        for row, row_ref in zip(df.collect(), df_ref.collect()):
            self.assertEqual(row['did'], row_ref['did'])
            self.assertEqual(row['did_bucket'], row_ref['did_bucket'])
            top_n = row['top_n_similar_user']
            top_n_ref = row_ref['top_n_similar_user']
            top_n_ordered = []
            
            # Convert the reference list into a list of score/[did] tuples.
            for ref in top_n_ref:
                if len(top_n_ordered) == 0 or float(ref['score']) != top_n_ordered[-1][0]:
                    top_n_ordered.append((float(ref['score']), [ ref['did'] ]))
                else:
                    top_n_ordered[-1][1].append(ref['did'])
            print(top_n_ordered)

            # Verify the similarity order and values.            
            index = 0
            index_count = 0
            for item in top_n:
                self.assertAlmostEqual(item['score'], top_n_ordered[index][0], 2)
                self.assertIn(item['did'], top_n_ordered[index][1])
                index_count += 1
                if (index_count == len(top_n_ordered[index][1])):
                    index += 1
                    index_count = 0

            
                



# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()