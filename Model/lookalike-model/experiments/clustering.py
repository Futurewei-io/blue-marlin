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

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.ml.clustering import KMeans
# import numpy as np

# Experiments with clustering methods.

score_table = 'from lookalike_application_score_vector_08192021_1m'
num_clusters = 100

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# num_features = 10
# num_rows = 10000
# data = np.random.rand(num_rows, num_features)
# spark.createDataFrame(data)

did_bucket = 0

command = "SELECT did, score_vector, did_bucket FROM {} WHERE did_bucket = {}".format(score_table, did_bucket)
df = spark.sql(command)

list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
df = df.withColumn('score_vec', list_to_vector_udf(df.score_vector))

first_time = True
if first_time:
    kmeans = KMeans(k=num_clusters, featuresCol='score_vec')
    kmeans.setSeed(1)
    kmeans.setPredictionCol('cluster_id')
    model = kmeans.fit(df)
    first_time = False

df2 = model.transform(df)
df2.show()





