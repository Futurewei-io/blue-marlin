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
from lookalike_model.application.pipeline import util, score_matrix_table


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
        with open('application/pipeline/config_score_matrix_table.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)

        # Get the names of the input and output tables.
        vector_table = cfg['score_vector']['score_vector_table']
        matrix_table = cfg['score_matrix_table']['score_matrix_table']

        # Create the input table.
        self.create_vector_table(vector_table, True)

        # Run the function being tested.
        score_matrix_table.run(self.spark, self.hive_context, cfg)

        # Load the output of the function.
        command = """select * from {} order by did_bucket""".format(matrix_table)
        df = self.hive_context.sql(command)
        df.show()

        command = """select * from {} order by did_bucket""".format(vector_table)
        df_vector = self.hive_context.sql(command)

        # Validate the output.
        self.validate_similarity_table(df, df_vector, True)

    def test_run2(self):
        print('*** Running TestTopNSimilarityTableGenerator.test_run2 ***')

        # Load the test configuration.
        with open('application/pipeline/config_score_matrix_table.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)

        # Get the names of the input and output tables.
        vector_table = cfg['score_vector']['score_vector_table']
        matrix_table = cfg['score_matrix_table']['score_matrix_table']
        
        cfg['score_matrix_table']['did_bucket_size'] = 2

        # Create the input table.
        self.create_vector_table(vector_table, False)

        # Run the function being tested.
        score_matrix_table.run(self.spark, self.hive_context, cfg)

        # Load the output of the function.
        command = """select * from {} order by did_bucket""".format(matrix_table)
        df = self.hive_context.sql(command)
        df.show()

        command = """select * from {} order by did_bucket""".format(vector_table)
        df_vector = self.hive_context.sql(command)

        # Validate the output.
        self.validate_similarity_table(df, df_vector, False)

    def test_run3(self):
        print('*** Running TestTopNSimilarityTableGenerator.test_run3 ***')

        # Load the test configuration.
        with open('application/pipeline/config_score_matrix_table.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        util.resolve_placeholder(cfg)

        # Get the names of the input and output tables.
        vector_table = cfg['score_vector']['score_vector_table']
        matrix_table = cfg['score_matrix_table']['score_matrix_table']
        
        cfg['score_matrix_table']['did_bucket_size'] = 2
        cfg['score_matrix_table']['did_bucket_step'] = 2

        # Create the input table.
        self.create_vector_table(vector_table, False)

        # Run the function being tested.
        score_matrix_table.run(self.spark, self.hive_context, cfg)

        # Load the output of the function.
        command = """select * from {} order by did_bucket""".format(matrix_table)
        df = self.hive_context.sql(command)
        df.show()

        command = """select * from {} order by did_bucket""".format(vector_table)
        df_vector = self.hive_context.sql(command)

        # Validate the output.
        self.validate_similarity_table(df, df_vector, False)

    def create_vector_table (self, table_name, same_bucket=False):
        later_bucket = 1
        if same_bucket:
            later_bucket = 0

        data = [
            ('0000001', [0.1, 0.8, 0.9], 1.46, 0),
            ('0000002', [0.1, 0.1, 0.1], 0.03, 0),
            ('0000003', [0.1, 0.8, 0.9], 1.46, later_bucket),
            ('0000004', [0.1, 0.2, 0.3], 0.14, later_bucket),
        ]

        schema = StructType([
            StructField("did", StringType(), True),
            StructField("score_vector", ArrayType(FloatType(), True), True),
            StructField("c1", FloatType(), True),
            StructField("did_bucket", IntegerType(), True)
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        util.write_to_table(df, table_name)

    def create_matrix(self):
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
        return df

    def create_matrix2(self):
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
        return df

    def validate_similarity_table (self, df, df_vector, same_bucket=False):
        if same_bucket:
            df_ref = self.create_matrix()
        else:
            df_ref = self.create_matrix2()

        # Verify the column names.
        self.assertEqual(len(df.columns), len(df_ref.columns))
        for name in df.columns:
            self.assertIn(name, df_ref.columns)

        # Verify the number of rows.
        self.assertEqual(df.count(), df_ref.count())

        # Index the score vector by did_bucket and did for quick reference.
        vectors = {}
        for row in df_vector.collect():
            did = row['did']
            score_vector = row['score_vector']
            c1 = row['c1']
            did_bucket = row['did_bucket']
            if did_bucket not in vectors:
                vectors[did_bucket] = {did: (score_vector, c1)}
            else:
                vectors[did_bucket][did] = (score_vector, c1)

        # Check the row values.
        for row in df.collect():
            did_bucket = row['did_bucket']
            self.assertIn(did_bucket, vectors)
            did_map = vectors[did_bucket]
            for did, vector, c1 in zip(row['did_list'], row['score_matrix'], row['c1_list']):
                self.assertIn(did, did_map)
                self.assertEqual(vector, did_map[did][0])
                self.assertEqual(c1, did_map[did][1])


# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()