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
from lookalike_model.application.pipeline import util, top_n_similarity

# To run:
# spark-submit --executor-memory 16G --driver-memory 16G --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict test_top_n_similarity_table_generator_2.py

class TestTopNSimilarityTableGenerator(unittest.TestCase):

    def setUp (self):
        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').config(
            "spark.archives",  # 'spark.yarn.dist.archives' in YARN.
            # "spark.yarn.dist.archives",  # 'spark.yarn.dist.archives' in YARN.
            "lookalike-application-python-venv.tar.gz#environment").enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')

    def test_run1(self):
        print('*** Running TestTopNSimilarityTableGenerator 2: test_run1 ***')

        # Load the test configuration.
        with open('application/pipeline/config_top_n_similarity.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)

        # Get the names of the input and output tables.
        vector_table = cfg['score_vector']['score_vector_table']
        top_n_table = cfg['top_n_similarity']['similarity_table']

        # Create the input table.
        self.create_vector_table(vector_table)

        # Run the function being tested.
        top_n_similarity.run(self.spark, cfg)

        # Load the output of the function.
        command = """select * from {} order by did""".format(top_n_table)
        df = self.spark.sql(command)
        df.show()

        # Validate the output.
        self.validate_similarity_table(df)

    def test_run2(self):
        print('*** Running TestTopNSimilarityTableGenerator 2: test_run2 ***')

        # Load the test configuration.
        with open('application/pipeline/config_top_n_similarity.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)
        cfg['top_n_similarity']['did_bucket_size'] = 4
        cfg['top_n_similarity']['search_bucket_size'] = 4

        # Get the names of the input and output tables.
        vector_table = cfg['score_vector']['score_vector_table']
        top_n_table = cfg['top_n_similarity']['similarity_table']

        # Create the input table.
        self.create_vector_table(vector_table, True)

        # Run the function being tested.
        top_n_similarity.run(self.spark, cfg)

        # Load the output of the function.
        command = """select * from {} order by did""".format(top_n_table)
        df = self.spark.sql(command)
        df.show()

        # Validate the output.
        self.validate_similarity_table(df, True)

    def test_run3(self):
        print('*** Running TestTopNSimilarityTableGenerator 2: test_run3 ***')

        # Load the test configuration.
        with open('application/pipeline/config_top_n_similarity.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)
        cfg['top_n_similarity']['did_bucket_size'] = 4
        cfg['top_n_similarity']['search_bucket_step'] = 2

        # Get the names of the input and output tables.
        vector_table = cfg['score_vector']['score_vector_table']
        top_n_table = cfg['top_n_similarity']['similarity_table']

        # Create the input table.
        self.create_vector_table(vector_table, True)

        # Run the function being tested.
        top_n_similarity.run(self.spark, cfg)

        # Load the output of the function.
        command = """select * from {} order by did""".format(top_n_table)
        df = self.spark.sql(command)
        df.show()

        # Validate the output.
        self.validate_similarity_table(df, True)

    def create_vector_table (self, table_name, separate_buckets=False):
        data = [
            ('0000001', [0.1, 0.8, 0.9], 1.46, 0),
            ('0000002', [0.1, 0.1, 0.1], 0.03, 1 if separate_buckets else 0),
            ('0000003', [0.1, 0.8, 0.9], 1.46, 2 if separate_buckets else 0),
            ('0000004', [0.1, 0.2, 0.3], 0.14, 3 if separate_buckets else 0),
        ]

        schema = StructType([
            StructField("did", StringType(), True),
            StructField("score_vector", ArrayType(FloatType(), True), True),
            StructField("c1", FloatType(), True),
            StructField("did_bucket", IntegerType(), True)
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        util.write_to_table(df, table_name)

    def validate_similarity_table (self, df, separate_bucket=False):
        data = [
            ('0000001', [{'did':'0000001', 'score':1.73205081}, {'did':'0000003', 'score':1.73205081}, {'did':'0000004', 'score':0.88532267}, {'did':'0000002', 'score':0.66903623}], 0),
            ('0000002', [{'did':'0000002', 'score':1.73205081}, {'did':'0000004', 'score':1.50844401}, {'did':'0000001', 'score':0.66903623}, {'did':'0000003', 'score':0.66903623}], 1 if separate_bucket else 0),
            ('0000003', [{'did':'0000001', 'score':1.73205081}, {'did':'0000003', 'score':1.73205081}, {'did':'0000004', 'score':0.88532267}, {'did':'0000002', 'score':0.66903623}], 2 if separate_bucket else 0),
            ('0000004', [{'did':'0000004', 'score':1.73205081}, {'did':'0000002', 'score':1.50844401}, {'did':'0000001', 'score':0.88532267}, {'did':'0000003', 'score':0.88532267}], 3 if separate_bucket else 0)
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

            # Because some users may be the equally distant from another user, 
            # group the users by the distance.  Convert the reference list 
            # into a list of score/[did] tuples.
            for ref in top_n_ref:
                if len(top_n_ordered) == 0 or float(ref['score']) != top_n_ordered[-1][0]:
                    top_n_ordered.append((float(ref['score']), [ ref['did'] ]))
                else:
                    top_n_ordered[-1][1].append(ref['did'])
            print(top_n_ordered)

            # Verify the similarity order and values.            
            index = 0
            index_count = 0
            prev_did = None
            prev_score = None
            has_repeated = False
            for item in top_n:
                if item['did'] == prev_did:
                    has_repeated = True

                    # If it is a repeated DID, it should be the same score.
                    self.assertEqual(item['score'], prev_score)
                    continue
                prev_did = item['did']
                prev_score = item['score']

                # If the number of users is less than N, the top N code will 
                # fill the rest of the results with the last entry.  So, once 
                # we see a repeated DID, we should only ever see repeated DIDs.
                self.assertFalse(has_repeated)

                # Check the order of the users in the top N 
                # self.assertAlmostEqual(item['score'], top_n_ordered[index][0], 2)
                self.assertIn(item['did'], top_n_ordered[index][1])
                index_count += 1
                if (index_count == len(top_n_ordered[index][1])):
                    index += 1
                    index_count = 0

            
                



# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()