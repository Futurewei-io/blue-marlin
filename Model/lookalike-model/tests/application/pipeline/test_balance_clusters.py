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
import random

from pyspark.sql import SparkSession

from pyspark.sql.types import DataType, StringType, StructField, StructType, FloatType, IntegerType, ArrayType, MapType
from lookalike_model.application.pipeline_v2 import util, balance_clusters

class TestBalanceClusters (unittest.TestCase):

    def setUp (self):
        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')

    def test_run (self):
        print('*** Running TestBalanceClusters.test_run ***')

        # Create the config file.
        cfg = {
            'score_vector_clustered': {
                'score_vector_cluster_table': 'lookalike_application_unittest_balance_clusters_input_score_vector'
            },
            'balance_clusters': {
                'target_cluster_size': 1000,
                'balanced_cluster_table': 'lookalike_application_unittest_balance_clusters_output_score_vector_balanced'
            }
        }

        # Get the names of the input and output tables.
        input_table = cfg['score_vector_clustered']['score_vector_cluster_table']
        output_table = cfg['balance_clusters']['balanced_cluster_table']
        target_cluster_size = cfg['balance_clusters']['target_cluster_size']

        # Create the input table.
        self.create_vector_table(input_table)

        # Run the function being tested.
        balance_clusters.run(self.spark, cfg)

        # Load the input of the function.
        command = """select * from {}""".format(input_table)
        df = self.spark.sql(command)
        total_users = df.count()
        # df.show()

        # Load the output of the function.
        command = """select * from {}""".format(output_table)
        df = self.spark.sql(command)
        # df.show()

        # Verify the output.
        self.validate_output(df, target_cluster_size, total_users)


    def create_vector_table (self, table_name):
        sizes = range(100, 2000, 100) + range(2000, 8000, 500)
        sizes.append(1500)  # Test cluster ID assignment corner case.
        num_features = 4
        clusters = [ [ 
                ( '{:08d}'.format(i), 
                    [ random.random() for _ in range(num_features) ], 
                    random.random(), 0, cluster ) 
                for i in range(size) ] 
            for cluster, size in enumerate(sizes) ]

        # Flatten the clusters.
        data = sum(clusters, [])

        schema = StructType([
            StructField("did", StringType(), True),
            StructField("score_vector", ArrayType(FloatType(), True), True),
            StructField("c1", FloatType(), True),
            StructField("did_bucket", IntegerType(), True),
            StructField("cluster_id", IntegerType(), True),
        ])

        df = self.spark.createDataFrame(self.spark.sparkContext.parallelize(data), schema)
        util.write_to_table(df, table_name)

    def validate_output (self, df, target_cluster_size, total_users):
        df_count = df.groupBy('cluster_id').count()
        df_count.orderBy('cluster_id').show(100)

        # Ensure output clusters are correct size.
        total = 0
        num_less = 0
        for row in df_count.collect():
            total += row['count']
            if row['count'] < 0.9 * target_cluster_size:  # A fudge factor since the splits are random.
                num_less += 1
            self.assertLess(row['count'], 2*target_cluster_size)

        # Allow at most only 1 cluster to be less than target size.
        self.assertLessEqual(num_less, 1)

        # Ensure no lost or gained users.
        self.assertEqual(total, total_users)

# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()

